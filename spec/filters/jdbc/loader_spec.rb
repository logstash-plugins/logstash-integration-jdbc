# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc/loader"

describe LogStash::Filters::Jdbc::Loader do
  let(:local_table) { "servers" }
  let(:options) do
    {
      "jdbc_driver_class" => "org.postgresql.Driver",
      "jdbc_connection_string" => "jdbc:postgres://user:password@remotedb/infra",
      "query" => "select ip, name, location from INTERNAL.SERVERS",
      "jdbc_user" => "bob",
      "jdbc_password" => "letmein",
      "max_rows" => 2000,
      "local_table" => local_table
    }
  end
  subject { described_class.new(options) }

  context "when correct options are given" do
    it "validation succeeds" do
      expect(subject.valid?).to be_truthy
      expect(subject.formatted_errors).to eq("")
    end
  end

  context "when incorrect options are given" do
    let(:options) do
      {
        "jdbc_driver_class" => 42,
        "jdbc_connection_string" => 42,
        "max_rows" => ["2000"]
      }
    end

    it "validation fails" do
      expect(subject.valid?).to be_falsey
      expect(subject.formatted_errors).to eq("The options must include a 'local_table' string, The options for '' must include a 'query' string, The 'max_rows' option for '' must be an integer, The 'jdbc_driver_class' option for '' must be a string, The 'jdbc_connection_string' option for '' must be a string")
    end
  end

  context "attr_reader methods" do
    it "#table" do
      expect(subject.table).to eq(:servers)
    end

    it "#query" do
      expect(subject.query).to eq("select ip, name, location from INTERNAL.SERVERS")
    end

    it "#max_rows" do
      expect(subject.max_rows).to eq(2000)
    end

    it "#connection_string" do
      expect(subject.connection_string).to eq("jdbc:postgres://user:password@remotedb/infra")
    end

    it "#driver_class" do
      expect(subject.driver_class).to eq("org.postgresql.Driver")
    end

    it "#driver_library" do
      expect(subject.driver_library).to be_nil
    end

    it "#user" do
      expect(subject.user).to eq("bob")
    end

    it "#password" do
      expect(subject.password.value).to eq("letmein")
    end
  end


end
