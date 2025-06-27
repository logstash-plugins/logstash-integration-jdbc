# encoding: utf-8
require "yaml" # persistence
require "date"
require "bigdecimal"

module LogStash module PluginMixins module Jdbc
  class ValueTracking

    def self.build_last_value_tracker(plugin)
      handler = plugin.record_last_run ? FileHandler.new(plugin.last_run_metadata_file_path) : NullFileHandler.new(plugin.last_run_metadata_file_path)
      if plugin.clean_run
        handler.clean
      end

      tracking_column_types = ['numeric', 'uuid']
      if plugin.use_column_value && tracking_column_types.include?(plugin.tracking_column_type)
        case plugin.tracking_column_type
        when "numeric"
          # use this irrespective of the jdbc_default_timezone setting
          NumericValueTracker.new(handler)
        when "uuid"
          UuidValueTracker.new(handler)
        end
      else
        if plugin.jdbc_default_timezone.nil?
          # no TZ stuff for Sequel, use Time
          TimeValueTracker.new(handler)
        else
          # Sequel does timezone handling on DateTime only
          DateTimeValueTracker.new(handler)
        end
      end
    end

    attr_reader :value

    def initialize(handler)
      @file_handler = handler
      set_initial
    end

    if Psych::VERSION&.split('.')&.first.to_i >= 4
      YAML_PERMITTED_CLASSES = [::DateTime, ::Time, ::BigDecimal, ::String].freeze
      def self.load_yaml(source)
        Psych::safe_load(source, permitted_classes: YAML_PERMITTED_CLASSES)
      end
    else
      def self.load_yaml(source)
        YAML::load(source)
      end
    end

    def set_initial
      # override in subclass
    end

    def set_value(value)
      # override in subclass
    end

    def write
      @file_handler.write(@value)
    end

    private
    def common_set_initial(method_symbol, default)
      persisted = @file_handler.read

      if persisted && persisted.respond_to?(method_symbol)
        @value = persisted
      else
        @file_handler.clean
        @value = default
      end
    end
  end


  class NumericValueTracker < ValueTracking
    def set_initial
      common_set_initial(:abs, 0)
    end

    def set_value(value)
      return unless value.is_a?(Numeric)
      @value = value
    end
  end

  class UuidValueTracker < ValueTracking
    def set_initial
      common_set_initial(:uuid, "00000000-0000-0000-0000-000000000000")
    end

    def set_value(value)
      return unless value.is_a?(String)
      @value = value
    end
  end

  class DateTimeValueTracker < ValueTracking
    def set_initial
      common_set_initial(:to_datetime, DateTime.new(1970))
    end

    def set_value(value)
      if value.respond_to?(:to_datetime)
        @value = value.to_datetime
      else
        @value = DateTime.parse(value)
      end
    end
  end

  class TimeValueTracker < ValueTracking
    def set_initial
      common_set_initial(:to_time, Time.at(0).utc)
    end

    def set_value(value)
      if value.respond_to?(:to_time)
        @value = value.to_time
      else
        @value = DateTime.parse(value).to_time
      end
    end
  end

  class FileHandler
    attr_reader :path

    def initialize(path)
      @path = path
      @exists = ::File.exist?(@path)
    end

    def clean
      return unless @exists
      ::File.delete(@path)
      @exists = false
    end

    def read
      return unless @exists
      ValueTracking.load_yaml(::File.read(@path))
    end

    def write(value)
      ::File.write(@path, YAML.dump(value))
      @exists = true
    end
  end

  class NullFileHandler
    def initialize(path)
    end

    def clean
    end

    def read
    end

    def write(value)
    end
  end
end end end
