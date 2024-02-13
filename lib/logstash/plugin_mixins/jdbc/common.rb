require 'jruby'

module LogStash module PluginMixins module Jdbc
  module Common

    private

    # NOTE: using the JRuby mechanism to load classes (through JavaSupport)
    # makes the lock redundant although it does not hurt to have it around.
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

      require_relative "sequel_bootstrap"
      require "sequel/adapters/jdbc"

      # execute all the driver loading related duties in a serial fashion to avoid
      # concurrency related problems with multiple pipelines and multiple drivers
      DRIVERS_LOADING_LOCK.lock()
      begin
        load_driver_jars
        begin
          @driver_impl = load_jdbc_driver_class
        rescue => e # catch java.lang.ClassNotFoundException, potential errors
          # (e.g. ExceptionInInitializerError or LinkageError) won't get caught
          message = if jdbc_driver_library_set?
                      "Are you sure you've included the correct jdbc driver in :jdbc_driver_library?"
                    else
                      ":jdbc_driver_library is not set, are you sure you included " +
                          "the proper driver client libraries in your classpath?"
                    end
          raise LogStash::PluginLoadingError, "#{e.inspect}. #{message}"
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

    def load_jdbc_driver_class
      # sub a potential: 'Java::org::my.Driver' to 'org.my.Driver'
      klass = @jdbc_driver_class.gsub('::', '.').sub(/^Java\./, '')
      # NOTE: JRuby's Java::JavaClass.for_name which considers the custom class-loader(s)
      # in 9.3 the API changed and thus to avoid surprises we go down to the Java API :
      klass = JRuby.runtime.getJavaSupport.loadJavaClass(klass) # throws ClassNotFoundException
      # unfortunately we can not simply return the wrapped java.lang.Class instance as
      # Sequel assumes to be able to do a `driver_class.new` which only works on the proxy,
      org.jruby.javasupport.Java.getProxyClass(JRuby.runtime, klass)
    end

  end
end end end
