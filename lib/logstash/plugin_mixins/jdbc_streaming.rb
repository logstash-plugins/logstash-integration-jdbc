# encoding: utf-8
require "logstash/config/mixin"

# Tentative of abstracting JDBC logic to a mixin
# for potential reuse in other plugins (input/output)
module LogStash module PluginMixins module JdbcStreaming
  class RowCache
    def initialize(size, ttl)
      @cache = ::LruRedux::TTL::ThreadSafeCache.new(size, ttl)
    end

    def get(parameters)
      @cache.getset(parameters) { yield }
    end
  end

  class NoCache
    def initialize(size, ttl) end

    def get(statement)
      yield
    end
  end

  # This method is called when someone includes this module
  def self.included(base)
    # Add these methods to the 'base' given.
    base.extend(self)
    base.setup_jdbc_config
  end

  public
  def setup_jdbc_config
    # JDBC driver library path to third party driver library.
    config :jdbc_driver_library, :validate => :path

    # JDBC driver class to load, for example "oracle.jdbc.OracleDriver" or "org.apache.derby.jdbc.ClientDriver"
    config :jdbc_driver_class, :validate => :string, :required => true

    # JDBC connection string
    config :jdbc_connection_string, :validate => :string, :required => true

    # JDBC user
    config :jdbc_user, :validate => :string

    # JDBC password
    config :jdbc_password, :validate => :password

    # Connection pool configuration.
    # Validate connection before use.
    config :jdbc_validate_connection, :validate => :boolean, :default => false

    # Connection pool configuration.
    # How often to validate a connection (in seconds)
    config :jdbc_validation_timeout, :validate => :number, :default => 3600
  end

  public
  def prepare_jdbc_connection
    load_driver

    @database = Sequel.connect(@jdbc_connection_string, complete_sequel_opts)
    if @jdbc_validate_connection
      @database.extension(:connection_validator)
      @database.pool.connection_validation_timeout = @jdbc_validation_timeout
    end
    begin
      @database.test_connection
    rescue Sequel::DatabaseConnectionError => e
      #TODO return false and let the plugin raise a LogStash::ConfigurationError
      raise e
    end
  end # def prepare_jdbc_connection
end end end
