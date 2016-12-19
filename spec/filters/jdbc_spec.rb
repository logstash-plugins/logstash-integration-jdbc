require 'spec_helper'
require "logstash/filters/jdbc"
require 'jdbc/derby'
describe LogStash::Filters::Jdbc do
  #Use embedded Derby for tests
  Jdbc::Derby.load_driver

  describe "All default - Retrieve a value from database" do
    let(:config) do <<-CONFIG
      filter {
        jdbc {
          jdbc_driver_class => "org.apache.derby.jdbc.EmbeddedDriver"
          jdbc_connection_string => "jdbc:derby:memory:testdb;create=true"
          statement => "SELECT 'from_database' FROM SYSIBM.SYSDUMMY1"
          target => "new_field"
        }
      }
    CONFIG
    end

    sample("message" => "some text") do
      expect(subject.get('new_field')).to eq([{"1" => 'from_database'}])
    end
  end

  describe "Named column - Retrieve a value from database" do
    let(:config) do <<-CONFIG
      filter {
        jdbc {
          jdbc_driver_class => "org.apache.derby.jdbc.EmbeddedDriver"
          jdbc_connection_string => "jdbc:derby:memory:testdb;create=true"
          statement => "SELECT 'from_database' as col_1 FROM SYSIBM.SYSDUMMY1"
          target => "new_field"
        }
      }
    CONFIG
    end

    sample("message" => "some text") do
      expect(subject.get('new_field')).to eq([{"col_1" => 'from_database'}])
    end
  end

  describe "Using string parameters - Retrieve a value from database" do
    let(:config) do <<-CONFIG
      filter {
        jdbc {
          jdbc_driver_class => "org.apache.derby.jdbc.EmbeddedDriver"
          jdbc_connection_string => "jdbc:derby:memory:testdb;create=true"
          statement => "SELECT 'from_database' FROM SYSIBM.SYSDUMMY1 WHERE '1' = :param"
          parameters => { "param" => "param_field"}
          target => "new_field"
        }
      }
    CONFIG
    end

    sample("message" => "some text", "param_field" => "1") do
      expect(subject.get('new_field')).to eq([{"1" => 'from_database'}])
    end

    sample("message" => "some text", "param_field" => "2") do
      expect(subject.get('new_field').nil?)
    end
  end

  describe "Using integer parameters" do
    let(:config) do <<-CONFIG
      filter {
        jdbc {
          jdbc_driver_class => "org.apache.derby.jdbc.EmbeddedDriver"
          jdbc_connection_string => "jdbc:derby:memory:testdb;create=true"
          statement => "SELECT 'from_database' FROM SYSIBM.SYSDUMMY1 WHERE 1 = :param"
          parameters => { "param" => "param_field"}
          target => "new_field"
        }
      }
    CONFIG
    end

    sample("message" => "some text", "param_field" => 1) do
      expect(subject.get('new_field')).to eq([{"1" => 'from_database'}])
    end

    sample("message" => "some text", "param_field" => "1") do
      expect(subject.get('new_field').nil?)
    end
  end

  describe "Using timestamp parameter" do
    let(:config) do <<-CONFIG
      filter {
        jdbc {
          jdbc_driver_class => "org.apache.derby.jdbc.EmbeddedDriver"
          jdbc_connection_string => "jdbc:derby:memory:testdb;create=true"
          statement => "SELECT 'from_database' FROM SYSIBM.SYSDUMMY1 WHERE {fn TIMESTAMPDIFF( SQL_TSI_DAY, {t :param}, current_timestamp)} = 0"
          parameters => { "param" => "@timestamp"}
          target => "new_field"
        }
      }
    CONFIG
    end

    sample("message" => "some text") do
      expect(subject.get('new_field')).to eq([{"1" => 'from_database'}])
    end
  end
end

