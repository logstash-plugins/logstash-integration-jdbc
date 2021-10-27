
module LogStash module PluginMixins module Jdbc
  module Common

    private

    DRIVERS_LOADING_LOCK = java.util.concurrent.locks.ReentrantLock.new()

    def complete_sequel_opts(defaults = {})
      sequel_opts = @sequel_opts.
          map { |key,val| [key.is_a?(String) ? key.to_sym : key, val] }.
          map { |key,val| [key, val.eql?('true') ? true : (val.eql?('false') ? false : val)] }
      sequel_opts = defaults.merge Hash[sequel_opts]
      sequel_opts[:user] = @jdbc_user unless @jdbc_user.nil? || @jdbc_user.empty?
      sequel_opts[:password] = @jdbc_password.value unless @jdbc_password.nil?
      sequel_opts[:driver] = @driver_impl # Sequel uses this as a fallback, if given URI doesn't auto-load the driver correctly
      sequel_opts
    end

    def load_driver
      return @driver_impl if @driver_impl ||= nil

      require "java"
      require "sequel"
      require "sequel/adapters/jdbc"

      # execute all the driver loading related duties in a serial fashion to avoid
      # concurrency related problems with multiple pipelines and multiple drivers
      DRIVERS_LOADING_LOCK.lock()
      begin
        load_driver_jars
        begin
          @driver_impl = Sequel::JDBC.load_driver(normalized_driver_class)
        rescue Sequel::AdapterNotFound => e # Sequel::AdapterNotFound, "#{@jdbc_driver_class} not loaded"
          # fix this !!!
          message = if jdbc_driver_library_set?
                      "Are you sure you've included the correct jdbc driver in :jdbc_driver_library?"
                    else
                      ":jdbc_driver_library is not set, are you sure you included " +
                          "the proper driver client libraries in your classpath?"
                    end
          raise LogStash::PluginLoadingError, "#{e}. #{message} #{e.backtrace}"
        end
      ensure
        DRIVERS_LOADING_LOCK.unlock()
      end
    end

    def load_driver_jars
      if jdbc_driver_library_set?
        @jdbc_driver_library.split(",").each do |driver_jar|
          @logger.debug("loading #{driver_jar}")
          # load 'driver.jar' is different than load 'some.rb' as it only causes the file to be added to
          # JRuby's class-loader lookup (class) path - won't raise a LoadError when file is not readable
          unless FileTest.readable?(driver_jar)
            raise LogStash::PluginLoadingError, "unable to load #{driver_jar} from :jdbc_driver_library, " +
                "file not readable (please check user and group permissions for the path)"
          end
          begin
            require driver_jar
          rescue LoadError => e
            raise LogStash::PluginLoadingError, "unable to load #{driver_jar} from :jdbc_driver_library, #{e.message}"
          rescue StandardError => e
            raise LogStash::PluginLoadingError, "unable to load #{driver_jar} from :jdbc_driver_library, #{e}"
          end
        end
      end
    end

    def jdbc_driver_library_set?
      !@jdbc_driver_library.nil? && !@jdbc_driver_library.empty?
    end

    # normalizing the class name to always have a Java:: prefix
    # is helpful since JRuby is only able to directly load class names
    # whose top-level package is com, org, java, javax
    # There are many jdbc drivers that use cc, io, net, etc.
    def normalized_driver_class
      if @jdbc_driver_class.start_with?("Java")
        @jdbc_driver_class
      else
        "Java::#{@jdbc_driver_class}"
      end
    end
  end
end end end
