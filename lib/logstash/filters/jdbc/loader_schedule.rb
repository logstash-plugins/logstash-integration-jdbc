require_relative "validatable"

module LogStash module Filters module Jdbc
  class LoaderSchedule < Validatable
    attr_reader :schedule_frequency, :loader_schedule

    private

    def post_initialize
      if valid?
        # From the Rufus::Scheduler docs:
        # By default, rufus-scheduler sleeps 0.300 second between every step.
        # At each step it checks for jobs to trigger and so on.
        if @cronline.seconds.is_a?(Set)
          @schedule_frequency = 0.3
        else
          @schedule_frequency = 30
        end
      end
    end

    def parse_options
      @loader_schedule = @options

      unless @loader_schedule.is_a?(String)
        @option_errors << "The loader_schedule option must be a string"
      end

      begin
        @cronline = Rufus::Scheduler::CronLine.new(@loader_schedule)
      rescue => e
        @option_errors << "The loader_schedule option is invalid: #{e.message}"
      end

      @valid = @option_errors.empty?
    end
  end
end end end
