# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "logstash/plugin_mixins/jdbc_streaming"
require "lru_redux"

# This filter executes a SQL query and store the result set in the field
# specified as `target`.
# It will cache the results locally in an LRU cache with expiry
#
# For example you can load a row based on an id from in the event
#
# [source,ruby]
# filter {
#   jdbc_streaming {
#     jdbc_driver_library => "/path/to/mysql-connector-java-5.1.34-bin.jar"
#     jdbc_driver_class => "com.mysql.jdbc.Driver"
#     jdbc_connection_string => ""jdbc:mysql://localhost:3306/mydatabase"
#     jdbc_user => "me"
#     jdbc_password => "secret"
#     statement => "select * from WORLD.COUNTRY WHERE Code = :code"
#     parameters => { "code" => "country_code"}
#     target => "country_details"
#   }
# }
#
module LogStash module Filters class JdbcStreaming < LogStash::Filters::Base
  class CachePayload
    attr_reader :payload
    def initialize
      @failure = false
      @payload = []
    end

    def push(data)
      @payload << data
    end

    def failed!
      @failure = true
    end

    def failed?
      @failure
    end

    def empty?
      @payload.empty?
    end
  end

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

  include LogStash::PluginMixins::JdbcStreaming

  config_name "jdbc_streaming"

  # Statement to execute.
  # To use parameters, use named parameter syntax, for example "SELECT * FROM MYTABLE WHERE ID = :id"
  config :statement, :validate => :string, :required => true

  # Hash of query parameter, for example `{ "id" => "id_field" }`
  config :parameters, :validate => :hash, :default => {}

  # Define the target field to store the extracted result(s)
  # Field is overwritten if exists
  config :target, :validate => :string, :required => true

  # Define a default object to use when lookup fails to return a matching row.
  # ensure that the key names of this object match the columns from the statement
  config :default_hash, :validate => :hash, :default => {}

  # Append values to the `tags` field if sql error occured
  config :tag_on_failure, :validate => :array, :default => ["_jdbcstreamingfailure"]

  # Append values to the `tags` field if no record was found and default values were used
  config :tag_on_default_use, :validate => :array, :default => ["_jdbcstreamingdefaultsused"]

  # Enable or disable caching, boolean true or false, defaults to true
  config :use_cache, :validate => :boolean, :default => true

  # The minimum number of seconds any entry should remain in the cache, defaults to 5 seconds
  # A numeric value, you can use decimals for example `{ "cache_expiration" => 0.25 }`
  # If there are transient jdbc errors the cache will store empty results for a given
  # parameter set and bypass the jbdc lookup, this merges the default_hash into the event, until
  # the cache entry expires, then the jdbc lookup will be tried again for the same parameters
  # Conversely, while the cache contains valid results any external problem that would cause
  # jdbc errors, will not be noticed for the cache_expiration period.
  config :cache_expiration, :validate => :number, :default => 5.0

  # The maximum number of cache entries are stored, defaults to 500 entries
  # The least recently used entry will be evicted
  config :cache_size, :validate => :number, :default => 500

  # ----------------------------------------
  public

  def register
    convert_config_options
    prepare_connected_jdbc_cache
  end

  def filter(event)
    result = cache_lookup(event) # should return a JdbcCachePayload

    if result.failed?
      tag_failure(event)
    end

    if result.empty?
      tag_default(event)
      process_event(event, @default_array)
    else
      process_event(event, result.payload)
    end
  end

  # ----------------------------------------
  private

  def cache_lookup(event)
    params = prepare_parameters_from_event(event)
    @cache.get(params) do
      result = CachePayload.new
      begin
        query = @database[@statement, params] # returns a dataset
        @logger.debug? && @logger.debug("Executing JDBC query", :statement => @statement, :parameters => params)
        query.all do |row|
          result.push row.inject({}){|hash,(k,v)| hash[k.to_s] = v; hash} #Stringify row keys
        end
      rescue ::Sequel::Error => e
        # all sequel errors are a subclass of this, let all other standard or runtime errors bubble up
        result.failed!
        @logger.warn? && @logger.warn("Exception when executing JDBC query", :exception => e)
      end
      # if either of: no records or a Sequel exception occurs the payload is
      # empty and the default can be substituted later.
      result
    end
  end

  def prepare_parameters_from_event(event)
    @symbol_parameters.inject({}) do |hash,(k,v)|
      value = event.get(event.sprintf(v))
      hash[k] = value.is_a?(::LogStash::Timestamp) ? value.time : value
      hash
    end
  end

  def tag_failure(event)
    @tag_on_failure.each do |tag|
      event.tag(tag)
    end
  end

  def tag_default(event)
    @tag_on_default_use.each do |tag|
      event.tag(tag)
    end
  end

  def process_event(event, value)
    # use deep clone here so other filter function don't taint the cached payload by reference
    event.set(@target, ::LogStash::Util.deep_clone(value))
    filter_matched(event)
  end

  def convert_config_options
    # create these object once they will be cloned for every filter call anyway,
    # lets not create a new object for each
    @symbol_parameters = @parameters.inject({}) {|hash,(k,v)| hash[k.to_sym] = v ; hash }
    @default_array = [@default_hash]
  end

  def prepare_connected_jdbc_cache
    klass = @use_cache ? RowCache : NoCache
    @cache = klass.new(@cache_size, @cache_expiration)
    prepare_jdbc_connection
  end
end end end # class LogStash::Filters::Jdbc
