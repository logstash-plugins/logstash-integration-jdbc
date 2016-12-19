# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "logstash/plugin_mixins/jdbc"

# This filter executes a SQL query and store the result set in the field
# specified as `target`.
#
# For example you can load a row based on an id from in the event
#
# [source,ruby]
# filter {
#   jdbc {
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
class LogStash::Filters::Jdbc < LogStash::Filters::Base
  include LogStash::PluginMixins::Jdbc
  config_name "jdbc"

  # Statement to execute.
  # To use parameters, use named parameter syntax, for example "SELECT * FROM MYTABLE WHERE ID = :id"
  config :statement, :validate => :string, :required => true

  # Hash of query parameter, for example `{ "id" => "id_field" }`
  config :parameters, :validate => :hash, :default => {}

  # Target field to store the result set.
  # Field is overwritten if exists.
  config :target, :validate => :string, :required => true

  # Append values to the `tags` field if sql error occured
  config :tag_on_failure, :validate => :array, :default => ["_jdbcfailure"]


  public
  def register
    @logger = self.logger
    prepare_jdbc_connection()
  end # def register

  public
  def filter(event)
    result = []
    #Prepare parameters from event values
    params = @parameters.inject({}) {|hash,(k,v)| hash[k] = event.get(event.sprintf(v)) ; hash }
    #Execute statement and collect results
    success = execute_statement(@statement,params) do |row|
      result << row
    end
    if success
      event.set(@target, result)
      filter_matched(event)
    else
      @tag_on_failure.each do |tag|
        event.tag(tag)
      end
    end
  end # def filter
end # class LogStash::Filters::Jdbc
