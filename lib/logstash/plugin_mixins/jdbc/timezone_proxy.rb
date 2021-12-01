# encoding: utf-8

require 'tzinfo'

module LogStash module PluginMixins module Jdbc
  class TimezoneProxy < SimpleDelegator
    ##
    # Wraps a ruby timezone object in an object that has an explicit preference in time conversions
    # either for or against having DST enabled.
    #
    # @param timezone [String,TZInfo::Timezone]
    # @param dst_enabled_on_overlap [Boolean] (default: nil) when encountering an ambiguous time,
    # declare a preference for selecting the option with DST either enabled or disabled.
    def self.wrap(timezone, dst_enabled_on_overlap)
      timezone = ::TZInfo::Timezone.get(timezone) if timezone.kind_of?(String)
      dst_enabled_on_overlap.nil? ? timezone : new(timezone, dst_enabled_on_overlap)
    end

    def self.parse(timezone_spec)
      md = /\A(?<base_spec>[^\[]+)(\[prefer-dst:(?<prefer_dst_spec>true|false)\])?\z/.match(timezone_spec)

      timezone = TZInfo::Timezone.get(md[:base_spec])
      return timezone unless md[:prefer_dst_spec]

      wrap(timezone, md[:prefer_dst_spec] == 'true')
    end

    ##
    # @api private
    def initialize(timezone, dst_enabled_on_overlap)
      super(timezone) # SimpleDelegator
      @dst_enabled_on_overlap = dst_enabled_on_overlap
    end

    ##
    # @override `Timezone#period_for_local`
    # inject an implicit preference for DST being either enabled or disabled if called
    # without an explicit preference
    def period_for_local(value, dst_enabled_on_overlap=nil, &global_disambiguator)
      dst_enabled_on_overlap = @dst_enabled_on_overlap if dst_enabled_on_overlap.nil?
      __getobj__.period_for_local(value, dst_enabled_on_overlap, &global_disambiguator)
    end
  end
end; end; end