# encoding: utf-8
require_relative "validatable"
require "rufus/scheduler"

module LogStash module Filters module Jdbc
  class LoaderSchedule < Validatable
    attr_reader :loader_schedule

    def to_log_string
      if @cronline.frequency == @cronline.brute_frequency
        "#{@cronline.frequency} seconds"
      else
        "#{@cronline.frequency} (brute: #{@cronline.brute_frequency}) seconds"
      end
    end

    private

    def fugit_cron?
      defined?(::Fugit) && @cronline.is_a?(::Fugit::Cron)
    end

    # @overload
    def parse_options
      @loader_schedule = @options

      if @loader_schedule.is_a?(String)
        begin
          # Rufus::Scheduler 3.0 - 3.6 methods signature: parse_cron(o, opts)
          # since Rufus::Scheduler 3.7 methods signature: parse_cron(o, opts={})
          @cronline = Rufus::Scheduler.parse_cron(@loader_schedule, {})
        rescue => e
          @option_errors << "The loader_schedule option is invalid: #{e.message}"
        end
      else
        @option_errors << "The loader_schedule option must be a string"
      end

      @valid = @option_errors.empty?
    end
  end
end end end
