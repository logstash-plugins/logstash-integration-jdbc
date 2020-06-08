# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/util/password"
require "logstash/filters/jdbc/read_only_database"

module LogStash module Filters module Jdbc
  describe ReadOnlyDatabase do
    let(:db) { Sequel.connect('mock://mydb') }
    let(:connection_string) { "mock://mydb" }
    let(:driver_class) { "org.apache.derby.jdbc.EmbeddedDriver" }
    let(:driver_library) { nil }
    subject(:read_only_db) { described_class.create(connection_string, driver_class, driver_library) }
    let(:stub_driver_class) { double(driver_class).as_null_object }

    describe "basic operations" do
      describe "initializing" do
        before(:each) do
          allow(Sequel::JDBC).to receive(:load_driver).and_return(stub_driver_class)
        end

        it "tests the connection with defaults" do
          expect(Sequel::JDBC).to receive(:load_driver).once.with(driver_class)
          expect(Sequel).to receive(:connect).once.with(connection_string, {:test => true, :driver => stub_driver_class})
          expect(read_only_db.empty_record_set).to eq([])
        end

        context 'with fully-specified arguments' do
          let(:connection_string) { "a connection string" }
          let(:user) { "a user" }
          let(:password) { Util::Password.new("secret") }
          let(:driver_class) { "a driver class" }

          it "tests the connection" do
            expect(Sequel::JDBC).to receive(:load_driver).once.with(driver_class)
            expect(Sequel).to receive(:connect).once.with(connection_string, {:user => user, :password =>  password.value, :test => true, :driver => stub_driver_class}).and_return(db)
            described_class.create(connection_string, driver_class, nil, user, password)
          end
        end

        it "connects with defaults" do
          expect(Sequel::JDBC).to receive(:load_driver).once.with(driver_class)
          expect(Sequel).to receive(:connect).once.with(connection_string, {:test => true, :driver => stub_driver_class}).and_return(db)
          expect(Sequel).to receive(:connect).once.with(connection_string, {:driver => stub_driver_class}).and_return(db)
          expect(read_only_db.connected?).to be_falsey
          read_only_db.connect("a caller specific error message")
          expect(read_only_db.connected?).to be_truthy
        end
      end

      describe "methods" do
        let(:dataset) { double("Sequel::Dataset") }

        before(:each) do
          allow(Sequel::JDBC).to receive(:load_driver)
          allow(Sequel).to receive(:connect).thrice.and_return(db)
          allow(db).to receive(:[]).and_return(dataset)
          read_only_db.connect("a caller specific error message")
        end

        after(:each) do
          read_only_db.disconnect("a caller specific error message")
        end

        it "the count method gets a count from the dataset" do
          expect(dataset).to receive(:count).and_return(0)
          read_only_db.count("select * from table")
        end

        it "the query method gets all records from the dataset" do
          expect(dataset).to receive(:all).and_return(read_only_db.empty_record_set)
          read_only_db.query("select * from table")
        end
      end
    end
  end
end end end
