# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc_static"
require "sequel"
require "sequel/adapters/jdbc"
require "stud/temporary"

module LogStash module Filters
  describe JdbcStatic, :integration => true do

    before(:all) do
      @thread_abort = Thread.abort_on_exception
      Thread.abort_on_exception = true
    end

    let(:loader_statement) { "SELECT ip, name, location FROM reference_table" }
    let(:lookup_statement) { "SELECT * FROM servers WHERE ip LIKE :ip" }
    let(:parameters_rhs) { "%%{[ip]}" }
    let(:temp_import_path_plugin) { Stud::Temporary.pathname }
    let(:temp_import_path_rspec) { Stud::Temporary.pathname }

    ENV["TZ"] = "Etc/UTC"

    # For Travis and CI based on docker, we source from ENV
    jdbc_connection_string = ENV.fetch("PG_CONNECTION_STRING",
                                       "jdbc:postgresql://postgresql:5432") + "/jdbc_static_db?user=postgres"

    let(:local_db_objects) do
      [
        {"name" => "servers", "index_columns" => ["ip"], "columns" => [["ip", "varchar(64)"], ["name", "varchar(64)"], ["location", "varchar(64)"]]},
      ]
    end

    let(:settings) do
      {
        "jdbc_user" => ENV['USER'],
        "jdbc_password" => ENV["POSTGRES_PASSWORD"],
        "jdbc_driver_class" => "org.postgresql.Driver",
        "jdbc_driver_library" => "/usr/share/logstash/postgresql.jar",
        "staging_directory" => temp_import_path_plugin,
        "jdbc_connection_string" => jdbc_connection_string,
        "loaders" => [
          {
            "id" =>"servers",
            "query" => loader_statement,
            "local_table" => "servers"
          }
        ],
        "local_db_objects" => local_db_objects,
        "local_lookups" => [
          {
            "query" => lookup_statement,
            "parameters" => {"ip" => parameters_rhs},
            "target" => "server"
          }
        ]
      }
    end

    let(:plugin) { JdbcStatic.new(settings) }

    let(:event) { ::LogStash::Event.new("message" => "some text", "ip" => ipaddr) }

    let(:ipaddr) { ".3.1.1" }

    describe "non scheduled operation" do
      after { plugin.close }

      context "under normal conditions" do
        it "enhances an event" do
          plugin.register
          plugin.filter(event)
          expect(event.get("server")).to eq([{"ip"=>"10.3.1.1", "name"=>"mv-server-1", "location"=>"MV-9-6-4"}])
        end
      end

      context "when the loader query returns no results" do
        let(:loader_statement) { "SELECT ip, name, location FROM reference_table WHERE ip LIKE '20%'" }
        it "add an empty array to the target field" do
          plugin.register
          plugin.filter(event)
          expect(event.get("server")).to eq([])
        end
      end

      context "under normal conditions with prepared statement" do
        let(:lookup_statement) { "SELECT * FROM servers WHERE ip LIKE ?" }
        let(:settings) do
          {
            "jdbc_user" => ENV['USER'],
            "jdbc_password" => ENV["POSTGRES_PASSWORD"],
            "jdbc_driver_class" => "org.postgresql.Driver",
            "jdbc_driver_library" => "/usr/share/logstash/postgresql.jar",
            "staging_directory" => temp_import_path_plugin,
            "jdbc_connection_string" => jdbc_connection_string,
            "loaders" => [
              {
                "id" =>"servers",
                "query" => loader_statement,
                "local_table" => "servers"
              }
            ],
            "local_db_objects" => local_db_objects,
            "local_lookups" => [
              {
                "query" => lookup_statement,
                "prepared_parameters" => [parameters_rhs],
                "target" => "server"
              }
            ]
          }
        end

        it "enhances an event" do
          plugin.register
          plugin.filter(event)
          expect(event.get("server")).to eq([{"ip"=>"10.3.1.1", "name"=>"mv-server-1", "location"=>"MV-9-6-4"}])
        end

        context 'and record with temporal columns' do
          let(:loader_statement) { "SELECT ip, name, location, entry_date, entry_time, timestamp FROM reference_table" }
          let(:local_db_objects) do
            [
              {
                "name" => "servers",
                "columns" => [
                  %w[ip varchar(64)],
                  %w[name varchar(64)],
                  %w[location varchar(64)],
                  %w[entry_date date],
                  %w[entry_time time],
                  %w[timestamp timestamp]
                ]
              },
            ]
          end

          before(:each) { plugin.register }

          subject { event.get("server").first }

          it "maps the DATE to a Logstash Timestamp" do
            plugin.filter(event)
            expect(subject['entry_date']).to eq(LogStash::Timestamp.new(Time.new(2003, 2, 1)))
          end

          it "maps the TIME field to a Logstash Timestamp" do
            plugin.filter(event)
            now = DateTime.now
            expect(subject['entry_time']).to eq(LogStash::Timestamp.new(Time.new(now.year, now.month, now.day, 10, 5, 0)))
          end

          it "maps the TIMESTAMP to a Logstash Timestamp" do
            plugin.filter(event)
            expect(subject['timestamp']).to eq(LogStash::Timestamp.new(Time.new(2003, 2, 1, 1, 2, 3)))
          end
        end
      end

      context "under normal conditions with prepared statement with multiple positional parameters" do

        let(:event) { LogStash::Event.new("host" => { "ip" => "10.3.1.1", "name" => "mv-server-1"}, "message" => "ping") }
        let(:lookup_statement) { "SELECT * FROM servers WHERE ip = ? AND name = ?" }

        let(:settings) do
          {
            "jdbc_user" => ENV['USER'],
            "jdbc_password" => ENV["POSTGRES_PASSWORD"],
            "jdbc_driver_class" => "org.postgresql.Driver",
            "jdbc_driver_library" => "/usr/share/logstash/postgresql.jar",
            "staging_directory" => temp_import_path_plugin,
            "jdbc_connection_string" => jdbc_connection_string,
            "loaders" => [
              {
                "id" =>"servers",
                "query" => loader_statement,
                "local_table" => "servers"
              }
            ],
            "local_db_objects" => local_db_objects,
            "local_lookups" => [
              {
                "query" => lookup_statement,
                "prepared_parameters" => ["[host][ip]", "[host][name]"],
                "target" => "server"
              }
            ]
          }
        end

        it "enhances an event" do
          plugin.register
          plugin.filter(event)
          expect(event.get("server")).to eq([{"ip"=>"10.3.1.1", "name"=>"mv-server-1", "location"=>"MV-9-6-4"}])
        end
      end

      context "under normal conditions with query with multiple named parameters" do

        let(:event) { LogStash::Event.new("host" => { "ip" => "10.3.1.1", "name" => "mv-server-1"}, "message" => "ping") }
        let(:lookup_statement) { "SELECT * FROM servers WHERE ip = :host_ip AND name = :host_name" }

        let(:settings) do
          {
            "jdbc_user" => ENV['USER'],
            "jdbc_password" => ENV["POSTGRES_PASSWORD"],
            "jdbc_driver_class" => "org.postgresql.Driver",
            "jdbc_driver_library" => "/usr/share/logstash/postgresql.jar",
            "staging_directory" => temp_import_path_plugin,
            "jdbc_connection_string" => jdbc_connection_string,
            "loaders" => [
              {
                "id" =>"servers",
                "query" => loader_statement,
                "local_table" => "servers"
              }
            ],
            "local_db_objects" => local_db_objects,
            "local_lookups" => [
              {
                "query" => lookup_statement,
                "parameters" => {
                  "host_ip" => "[host][ip]",
                  "host_name" =>"[host][name]"
                },
                "target" => "server"
              }
            ]
          }
        end

        it "enhances an event" do
          plugin.register
          plugin.filter(event)
          expect(event.get("server")).to eq([{"ip"=>"10.3.1.1", "name"=>"mv-server-1", "location"=>"MV-9-6-4"}])
        end
      end

      context "under normal conditions when index_columns is not specified" do
        let(:local_db_objects) do
          [
            {"name" => "servers", "columns" => [["ip", "varchar(64)"], ["name", "varchar(64)"], ["location", "varchar(64)"]]},
          ]
        end
        it "enhances an event" do
          plugin.register
          plugin.filter(event)
          expect(event.get("server")).to eq([{"ip"=>"10.3.1.1", "name"=>"mv-server-1", "location"=>"MV-9-6-4"}])
        end
      end
    end

    describe "scheduled operation" do
      context "given a loader_schedule" do
        it "should properly schedule" do
          settings["loader_schedule"] = "*/3 * * * * * UTC"
          static_filter = JdbcStatic.new(settings)
          runner = Thread.new(static_filter) do |filter|
            filter.register
          end
          runner.join(4)
          sleep 4
          static_filter.filter(event)
          expect(static_filter.loader_runner.reload_count).to be > 1
          static_filter.close
          expect(event.get("server")).to eq([{"ip"=>"10.3.1.1", "name"=>"mv-server-1", "location"=>"MV-9-6-4"}])
        end
      end
    end
  end
end end
