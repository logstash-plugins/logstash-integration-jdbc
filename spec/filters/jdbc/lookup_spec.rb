# encoding: utf-8
require_relative "../env_helper"
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc/lookup"

module LogStash module Filters module Jdbc
  describe Lookup do
    describe "class method find_validation_errors" do
      context "when supplied with an invalid arg" do
        it "nil as arg, fails validation" do
          result = described_class.find_validation_errors(nil)
          expect(result).to eq("The options must be an Array")
        end

        it "hash as arg, fails validation" do
          result = described_class.find_validation_errors({})
          expect(result).to eq("The options must be an Array")
        end

        it "array of lookup hash without query key as arg, fails validation" do
          lookup_hash = {
          "parameters" => {"ip" => "%%{[ip]}"},
          "target" => "server"
          }
          result = described_class.find_validation_errors([lookup_hash])
          expect(result).to eq("The options for 'lookup-1' must include a 'query' string")
        end

        it "array of lookup hash with bad parameters value as arg, fails validation" do
          lookup_hash = {
          "query" => "select * from servers WHERE ip LIKE :ip",
          "parameters" => %w(ip %%{[ip]}),
          "target" => "server"
          }
          result = described_class.find_validation_errors([lookup_hash])
          expect(result).to eq("The 'parameters' option for 'lookup-1' must be a Hash")
        end

        it "array of lookup hash with bad parameters value as arg and no target, fails validation" do
          lookup_hash = {
          "query" => "select * from servers WHERE ip LIKE :ip",
          "parameters" => %w(ip %%{[ip]})
          }
          result = described_class.find_validation_errors([lookup_hash])
          expect(result).to eq("The 'parameters' option for 'lookup-1' must be a Hash")
        end

        it "parameters and prepared_parameters are defined at same time" do
          lookup_hash = {
          "query" => "SELECT * FROM table WHERE ip=?",
          "parameters" => {"ip" => "%%{[ip]}"},
          "prepared_parameters" => ["%%{[ip]}"],
          "target" => "server"
          }
          result = described_class.find_validation_errors([lookup_hash])
          expect(result).to eq("Can't specify 'parameters' and 'prepared_parameters' in the same lookup")
        end

        it "prepared_parameters count doesn't match the number of '?' in the query" do
          lookup_hash = {
          "query" => "SELECT * FROM table WHERE ip=? AND host=?",
          "prepared_parameters" => ["%%{[ip]}"],
          "target" => "server"
          }
          result = described_class.find_validation_errors([lookup_hash])
          expect(result).to eq("The 'prepared_parameters' option for 'lookup-1' doesn't match count with query's placeholder")
        end
      end

      context "when supplied with a valid arg" do
        it "empty array as arg, passes validation" do
          result = described_class.find_validation_errors([])
          expect(result).to eq(nil)
        end

        it "array of valid lookup hash as arg, passes validation" do
          lookup_hash = {
            "query" => "select * from servers WHERE ip LIKE :ip",
            "parameters" => {"ip" => "%%{[ip]}"},
            "target" => "server"
          }
          result = described_class.find_validation_errors([lookup_hash])
          expect(result).to eq(nil)
        end
      end
    end

    describe "abnormal operations" do
      let(:local_db) { double("local_db") }
      let(:lookup_hash) do
        {
        "query" => "select * from servers WHERE ip LIKE :ip",
        "parameters" => {"ip" => "%%{[ip]}"},
        "target" => "server",
        "tag_on_failure" => ["_jdbcstaticfailure_server"]
        }
      end
      let(:event) { LogStash::Event.new()}
      let(:records) { [{"name" => "ldn-1-23", "rack" => "2:1:6"}] }

      subject(:lookup) { described_class.new(lookup_hash, {}, "lookup-1") }

      before(:each) do
        allow(local_db).to receive(:fetch).once.and_return(records)
      end

      it "should not enhance an event and it should tag" do
        subject.enhance(local_db, event)
        expect(event.get("tags")).to eq(["_jdbcstaticfailure_server"])
        expect(event.get("server")).to be_nil
      end
    end

    describe "normal operations" do
      let(:local_db) { double("local_db") }
      let(:lookup_hash) do
        {
          "query" => "select * from servers WHERE ip LIKE :ip",
          "parameters" => {"ip" => "%%{[ip]}"},
          "target" => "server",
          "tag_on_failure" => ["_jdbcstaticfailure_server"]
        }
      end
      let(:event) { LogStash::Event.new()}
      let(:records) { [{"name" => "ldn-1-23", "rack" => "2:1:6"}] }

      subject(:lookup) { described_class.new(lookup_hash, {}, "lookup-1") }

      before(:each) do
        allow(local_db).to receive(:fetch).once.and_return(records)
      end

      it "should be valid" do
        expect(subject.valid?).to be_truthy
      end

      it "should have no formatted_errors" do
        expect(subject.formatted_errors).to eq("")
      end

      it "should enhance an event" do
        event.set("ip", "20.20")
        subject.enhance(local_db, event)
        expect(event.get("tags")).to be_nil
        expect(event.get("server")).to eq(records)
      end
    end

    describe "lookup operations with prepared statement" do
      let(:local_db) { double("local_db") }
      let(:lookup_hash) do
        {
          "query" => "select * from servers WHERE ip LIKE ?",
          "prepared_parameters" => ["%%{[ip]}"],
          "target" => "server",
          "tag_on_failure" => ["_jdbcstaticfailure_server"]
        }
      end
      let(:event) { LogStash::Event.new()}
      let(:records) { [{"name" => "ldn-1-23", "rack" => "2:1:6"}] }
      let(:prepared_statement) { double("prepared_statement")}

      subject(:lookup) { described_class.new(lookup_hash, {}, "lookup-1") }

      before(:each) do
        allow(local_db).to receive(:prepare).once.and_return(prepared_statement)
        allow(prepared_statement).to receive(:call).once.and_return(records)
      end

      it "should be valid" do
        expect(subject.valid?).to be_truthy
      end

      it "should have no formatted_errors" do
        expect(subject.formatted_errors).to eq("")
      end

      it "should enhance an event" do
        event.set("ip", "20.20")
        subject.prepare(local_db)
        subject.enhance(local_db, event)
        expect(event.get("tags")).to be_nil
        expect(event.get("server")).to eq(records)
      end
    end

    describe "lookup operations with prepared statement multiple parameters" do
      let(:local_db) { double("local_db") }
      let(:lookup_hash) do
        {
          "query" => "select * from servers WHERE ip LIKE ? AND os LIKE ?",
          "prepared_parameters" => ["%%{[ip]}", "os"],
          "target" => "server",
          "tag_on_failure" => ["_jdbcstaticfailure_server"]
        }
      end
      let(:event) { LogStash::Event.new()}
      let(:records) { [{"name" => "ldn-1-23", "rack" => "2:1:6"}] }
      let(:prepared_statement) { double("prepared_statement")}

      subject(:lookup) { described_class.new(lookup_hash, {}, "lookup-1") }

      before(:each) do
        allow(local_db).to receive(:prepare).once.and_return(prepared_statement)
        allow(prepared_statement).to receive(:call).once.and_return(records)
      end

      it "should be valid" do
        expect(subject.valid?).to be_truthy
      end

      it "should have no formatted_errors" do
        expect(subject.formatted_errors).to eq("")
      end

      it "should enhance an event" do
        event.set("ip", "20.20")
        event.set("os", "MacOS")
        subject.prepare(local_db)
        subject.enhance(local_db, event)
        expect(event.get("tags")).to be_nil
        expect(event.get("server")).to eq(records)
      end
    end

    describe "lookup operations with badly configured prepared statement" do
      let(:local_db) { double("local_db") }
      let(:lookup_hash) do
        {
          "query" => "select * from servers WHERE ip LIKE ? AND os LIKE ?",
          "prepared_parameters" => ["%%{[ip]}"],
          "target" => "server",
          "tag_on_failure" => ["_jdbcstaticfailure_server"]
        }
      end
      let(:event) { LogStash::Event.new()}
      let(:records) { [{"name" => "ldn-1-23", "rack" => "2:1:6"}] }
      let(:prepared_statement) { double("prepared_statement")}

      subject(:lookup) { described_class.new(lookup_hash, {}, "lookup-1") }

      before(:each) do
        allow(local_db).to receive(:prepare).once.and_return(prepared_statement)
        allow(prepared_statement).to receive(:call).once.and_return(records)
      end

      it "must not be valid" do
        expect(subject.valid?).to be_falsey
      end
    end

    describe "validation of target option" do
      let(:lookup_hash) do
        {
          "query" => "select * from servers WHERE ip LIKE ? AND os LIKE ?",
          "prepared_parameters" => ["%%{[ip]}"],
        }
      end

      it "should log a warn when ECS is enabled and target not defined" do

        class LoggableLookup < Lookup

          @@TEST_LOGGER = nil

          def self.logger=(log)
            @@TEST_LOGGER = log
          end

          def self.logger
            @@TEST_LOGGER
          end
        end

        spy_logger = double("logger")
        expect(spy_logger).to receive(:info).once.with(/ECS compatibility is enabled but no .*?target.*? was specified/)
        LoggableLookup.logger = spy_logger

        LoggableLookup.new(lookup_hash, {:ecs_compatibility => 'v1'}, "lookup-1")
      end
    end
  end
end end end

