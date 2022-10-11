# encoding: utf-8

require 'tzinfo'

module LogStash module PluginMixins module Jdbc
  ##
  # This `TimezoneProxy` allows timezone specs to include extensions indicating preference for ambiguous handling.
  # @see TimezoneProxy::parse
  module TimezoneProxy
    ##
    # @param timezone_spec [String]: a timezone spec, consisting of any valid timezone identifier
    #                                followed by square-bracketed extensions. Currently-supported
    #                                extensions are:
    #                                `dst_enabled_on_overlap:(true|false)`: when encountering an ambiguous time
    #                                                                       due to daylight-savings transition,
    #                                                                       assume DST to be either enabled or
    #                                                                       disabled instead of raising an
    #                                                                       AmbiguousTime exception
    # @return [TZInfo::Timezone]
    def self.load(timezone_spec)
      # re-load pass-through
      return timezone_spec if timezone_spec.kind_of?(::TZInfo::Timezone)

      parsed_spec = /\A(?<name>[^\[]+)(\[(?<extensions>[^\]]*)\])?\z/.match(timezone_spec)

      timezone = ::TZInfo::Timezone.get(parsed_spec[:name])
      return timezone unless parsed_spec[:extensions]

      parsed_spec[:extensions].split(';').each do |extension_spec|
        timezone = case extension_spec
                   when 'dst_enabled_on_overlap:true'  then timezone.dup.extend(PeriodForLocalWithDSTPreference::ON)
                   when 'dst_enabled_on_overlap:false' then timezone.dup.extend(PeriodForLocalWithDSTPreference::OFF)
                   else fail(ArgumentError, "Invalid timezone extension `#{extension_spec}`")
                   end
      end

      timezone
    end

    module JDBCTimezoneSpecValidator
      def validate_value(value, validator_name)
        return super(value, validator_name) unless validator_name == :jdbc_timezone_spec

        [true, TimezoneProxy.load(value)] rescue [false, $!.message]
      end
    end

    ##
    # @api private
    class PeriodForLocalWithDSTPreference < Module
      def initialize(default_dst_enabled_on_overlap)
        define_method(:period_for_local) do |localtime, dst_enabled_on_overlap=nil, &dismabiguation_block|
          super(localtime, dst_enabled_on_overlap.nil? ? default_dst_enabled_on_overlap : dst_enabled_on_overlap, &dismabiguation_block)
        end
      end

      ON = new(true)
      OFF = new(false)
    end
  end
end; end; end
