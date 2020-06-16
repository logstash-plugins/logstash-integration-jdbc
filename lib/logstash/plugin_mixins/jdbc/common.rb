
module LogStash module PluginMixins module Jdbc
  module Common

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

  end
end end end
