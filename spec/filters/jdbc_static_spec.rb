# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc_static"
require "sequel"
require "sequel/adapters/jdbc"
require "stud/temporary"
require "timecop"
require "pathname"

# LogStash::Logging::Logger::configure_logging("WARN")

module LogStash module Filters
  describe JdbcStatic do

    before(:all) do
      @thread_abort = Thread.abort_on_exception
      Thread.abort_on_exception = true
    end

    let(:jdbc_connection_string) { "jdbc:derby:memory:jdbc_static_testdb;create=true" }
    let(:db1) { ::Sequel.connect(jdbc_connection_string, :user=> nil, :password=> nil) }
    let(:loader_statement) { "SELECT ip, name, location FROM reference_table" }
    let(:lookup_statement) { "SELECT * FROM servers WHERE ip LIKE :ip" }
    let(:parameters_rhs) { "%%{[ip]}" }
    let(:temp_import_path_plugin) { Stud::Temporary.pathname }
    let(:temp_import_path_rspec) { Stud::Temporary.pathname }

    let(:local_db_objects) do
      [
        {"name" => "servers", "index_columns" => ["ip"], "columns" => [["ip", "varchar(64)"], ["name", "varchar(64)"], ["location", "varchar(64)"]]},
      ]
    end

    let(:settings) do
      {
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

    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "#{jdbc_connection_string}",
        "staging_directory" => temp_import_path_plugin
      }
    end

    let(:add_records) do
      lambda do |fd|
        fd.puts "'10.1.1.1', 'ldn-server-1', 'LDN-2-3-4'"
        fd.puts "'10.2.1.1', 'nyc-server-1', 'NYC-5-2-8'"
        fd.puts "'10.3.1.1', 'mv-serv''r-1', 'MV-9-6-4'"
      end
    end

    let(:plugin) { JdbcStatic.new(mixin_settings.merge(settings)) }

    before do
      db1.drop_table(:reference_table) rescue nil
      db1.create_table(:reference_table) do
        String :ip
        String :name
        String :location
      end
      ::File.open(temp_import_path_rspec, "w") do |fd|
        add_records.call(fd)
      end
      import_cmd = "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (null,'REFERENCE_TABLE','#{temp_import_path_rspec}',null,'''',null,1)"
      db1.execute_ddl(import_cmd)
    end

    let(:event)      { ::LogStash::Event.new("message" => "some text", "ip" => ipaddr) }

    let(:ipaddr) { ".3.1.1" }

    describe "verify derby path property" do
      it "should be set into Logstash data path" do
        plugin.register

        expected = Pathname.new(LogStash::SETTINGS.get_value("path.data")).join("plugins", "shared", "derby_home").to_path
        expect(java.lang.System.getProperty("derby.system.home")).to eq(expected)
      end
    end

    describe "non scheduled operation" do
      after { plugin.close }

      context "under normal conditions" do
        it "enhances an event" do
          plugin.register
          plugin.filter(event)
          expect(event.get("server")).to eq([{"ip"=>"10.3.1.1", "name"=>"mv-serv'r-1", "location"=>"MV-9-6-4"}])
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

      context "when the loader query returns a large recordset, local db is filled in chunks" do
        let(:add_records) do
          lambda do |fd|
            256.times do |octet3|
              256.times do |octet4|
                fd.puts "'10.4.#{octet3}.#{octet4}', 'server-#{octet3}-#{octet4}', 'MV-10-#{octet3}-#{octet4}'"
              end
            end
          end
        end
        let(:ipaddr) { "10.4.254.255" }
        let(:lookup_statement) { "SELECT * FROM servers WHERE ip = :ip" }
        let(:parameters_rhs) { "ip" }
        it "enhances an event" do
          plugin.register
          plugin.filter(event)
          expect(event.get("server")).to eq([{"ip"=>ipaddr, "name"=>"server-254-255", "location"=>"MV-10-254-255"}])
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
          expect(event.get("server")).to eq([{"ip"=>"10.3.1.1", "name"=>"mv-serv'r-1", "location"=>"MV-9-6-4"}])
        end
      end
    end

    describe "scheduled operation" do
      context "given a loader_schedule" do
        it "should properly schedule" do
          settings["loader_schedule"] = "*/10 * * * * * UTC"
          Timecop.travel(Time.now.utc - 3600)
          Timecop.scale(60)
          static_filter = JdbcStatic.new(mixin_settings.merge(settings))
          runner = Thread.new(static_filter) do |filter|
            filter.register
          end
          sleep 3
          static_filter.filter(event)
          expect(static_filter.loader_runner.reload_count).to be > 1
          static_filter.close
          Timecop.return
          expect(event.get("server")).to eq([{"ip"=>"10.3.1.1", "name"=>"mv-serv'r-1", "location"=>"MV-9-6-4"}])
        end
      end
    end
  end
end end
