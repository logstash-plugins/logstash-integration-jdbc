# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash-integration-jdbc_jars"
require "logstash/util/password"
require "logstash/filters/jdbc/db_object"
require "logstash/filters/jdbc/read_write_database"
require "stud/temporary"

module LogStash module Filters module Jdbc
  describe ReadWriteDatabase do
    let(:db) { Sequel.connect('mock://mydb') }
    let(:connection_string_regex) { /jdbc:derby:memory:\w+;create=true/ }
    let(:temp_import_path_plugin) { Stud::Temporary.pathname }
    subject(:read_write_db) { described_class.create }

    describe "basic operations" do
      context "connecting  to a db" do
        it "connects with defaults" do
          expect(::Sequel::JDBC).to receive(:load_driver).once.with("org.apache.derby.jdbc.EmbeddedDriver")
          # two calls to connect because ReadWriteDatabase does verify_connection and connect
          expect(::Sequel).to receive(:connect).once.with(connection_string_regex, {:test => true}).and_return(db)
          expect(::Sequel).to receive(:connect).once.with(connection_string_regex, {}).and_return(db)
          expect(read_write_db.empty_record_set).to eq([])
        end

        it "connects with fully specified arguments" do
          connection_str = "a connection string"
          user = "a user"
          password = Util::Password.new("secret")
          expect(::Sequel::JDBC).to receive(:load_driver).once.with("a driver class")
          expect(::Sequel).to receive(:connect).once.with(connection_str, {:user => user, :password => password.value, :test => true}).and_return(db)
          expect(::Sequel).to receive(:connect).once.with(connection_str, {:user => user, :password => password.value}).and_return(db)
          described_class.create(connection_str, "a driver class", nil, user, password)
        end
      end

      describe "methods" do
        let(:dataset) { double("Sequel::Dataset") }
        let(:loaders) { [] }
        let(:loader)  { double("Loader") }
        let(:table_name) { "users" }
        let(:random_table_name)  { "foobarbaz" }
        let(:multi_insert_sql) { "INSERT VALUES (1, 2, 3)" }

        before(:each) do
          FileUtils.mkdir_p(temp_import_path_plugin)
          allow(::Sequel::JDBC).to receive(:load_driver)
          # allow(::Sequel).to receive(:connect).twice.and_return(db)
          allow(::Sequel).to receive(:connect).twice.and_return(db)
          allow(loader).to receive(:fetch).and_return([{:a => 1, :b => 2, :c => 3}])
          allow(loader).to receive(:table).and_return(table_name)
          allow(loader).to receive(:id).and_return("some_id")
          allow(loader).to receive(:staging_directory).and_return(temp_import_path_plugin)
          allow(described_class).to receive(:random_name).and_return(random_table_name)
          loaders.push(loader)
        end

        it "the populate_all method fills a local_db from the dataset" do
          expect(db).to receive(:[]).with(loader.table).once.and_return(dataset)
          expect(dataset).to receive(:literal).and_return(1.to_s, 2.to_s, 3.to_s)
          import_file_path = ::File.join(temp_import_path_plugin, loader.table)
          expect(db).to receive(:execute_ddl).once.with("CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (null,'#{loader.table.upcase}','#{import_file_path}',null,'''',null,1)")
          read_write_db.populate_all(loaders)
        end

        it "the repopulate_all method fills a local_db from the dataset" do
          expect(db).to receive(:[]).with(loader.table).once.and_return(dataset)
          expect(dataset).to receive(:literal).and_return(1.to_s, 2.to_s, 3.to_s)
          import_file_path = ::File.join(temp_import_path_plugin, loader.table)
          expect(db).to receive(:execute_ddl).once.with("CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (null,'#{loader.table.upcase}','#{import_file_path}',null,'''',null,1)")
          read_write_db.repopulate_all(loaders)
        end

        it "the fetch method executes a parameterised SQL statement on the local db" do
          statement = "select 1 from dual"
          parameters = 42
          expect(db).to receive(:[]).with(statement, parameters).once.and_return(dataset)
          expect(dataset).to receive(:all).once.and_return([1,2,3])
          read_write_db.fetch(statement, parameters)
        end

        it "lends the local db to a DbObject build instance method" do
          db_object = DbObject.new("type" => "index", "name" => "servers_idx", "table" => "servers", "columns" => ["ip"])
          expect(db_object).to receive(:build).once.with(db)
          read_write_db.build_db_object(db_object)
        end
      end
    end
  end
end end end
