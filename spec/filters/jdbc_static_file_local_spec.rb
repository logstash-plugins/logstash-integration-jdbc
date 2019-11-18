# encoding: utf-8
require_relative "env_helper"
require_relative "remote_server_helper"

require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc_static"
require "sequel"
require "sequel/adapters/jdbc"

module LogStash module Filters
  describe JdbcStatic, :skip => "temporary disable lookup_jdbc_* settings" do
    let(:db1) { ::Sequel.connect("jdbc:derby:memory:testdb;create=true", :user=> nil, :password=> nil) }
    let(:test_loader) { "SELECT * FROM reference_table" }
    let(:test_records) { db1[test_loader].all }
    let(:lookup_db) { "lookupdb" }

    let(:local_db_objects) do
      [
        {"name" => "servers", "preserve_existing" => true, "index_columns" => ["ip"], "columns" => [["ip", "varchar(64)"], ["name", "varchar(64)"], ["location", "varchar(64)"]]},
      ]
    end

    let(:settings) do
      {
        "loaders" => [
          {
            "id" =>"servers",
            "query" => "select ip, name, location from reference_table",
            "local_table" => "servers"
          }
        ],
        "local_db_objects" => local_db_objects,
        "local_lookups" => [
          {
            "query" => "select * from servers WHERE ip LIKE :ip",
            "parameters" => {"ip" => "%%{[ip]}"},
            "target" => "server"
          }
        ]
      }
    end

    let(:client_jar_path) { ::File.join(BASE_DERBY_DIR, "derbyclient.jar") }

    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true",
        "lookup_jdbc_driver_class" => "org.apache.derby.jdbc.ClientDriver",
        "lookup_jdbc_driver_library" => nil,
        "lookup_jdbc_connection_string" => "jdbc:derby://localhost:1527/#{lookup_db};create=true" }
    end
    let(:plugin) { JdbcStatic.new(mixin_settings.merge(settings)) }

    after do
      plugin.stop
      ServerProcessHelpers.jdbc_static_stop_derby_server(lookup_db)
    end

    before do
      ServerProcessHelpers.jdbc_static_start_derby_server
      db1.drop_table(:reference_table) rescue nil
      db1.create_table :reference_table do
        String :ip
        String :name
        String :location
      end
      db1[:reference_table].insert(:ip => "10.1.1.1", :name => "ldn-server-1", :location => "LDN-2-3-4")
      db1[:reference_table].insert(:ip => "10.2.1.1", :name => "nyc-server-1", :location => "NYC-5-2-8")
      db1[:reference_table].insert(:ip => "10.3.1.1", :name => "mv-server-1", :location => "MV-9-6-4")

      plugin.register
    end

    let(:event)      { ::LogStash::Event.new("message" => "some text", "ip" => ipaddr) }

    let(:ipaddr) { ".3.1.1" }

    it "enhances an event" do
      plugin.filter(event)
      expect(event.get("server")).to eq([{"ip"=>"10.3.1.1", "name"=>"mv-server-1", "location"=>"MV-9-6-4"}])
    end
  end
end end
