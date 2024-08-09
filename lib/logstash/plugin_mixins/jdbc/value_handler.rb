# encoding: utf-8
require "time"
require "date"

module LogStash module PluginMixins module Jdbc
  # Provides functions to extract the row's values, ensuring column types
  # are properly decorated to become coercible to a LogStash::Event.
  module ValueHandler
    # Stringify the row keys and decorate values when necessary
    def extract_values_from(row)
      Hash[row.map { |k, v| [k.to_s, decorate_value(v)] }]
    end

    # Decorate the value so it can be used as a LogStash::Event field
    def decorate_value(value)
      case value
      when Date, DateTime
        value.to_time
      else
        value
      end
    end
  end
end end end