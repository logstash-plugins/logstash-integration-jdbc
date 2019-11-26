# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc/db_object"

describe LogStash::Filters::Jdbc::DbObject  do
  context "various invalid non-hash arguments" do
    it "a nil does not validate" do
      instance = described_class.new(nil)
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq("DbObject options must be a Hash")
    end

    it "a string does not validate" do
      instance = described_class.new("foo")
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq("DbObject options must be a Hash")
    end

    it "a number does not validate" do
      instance = described_class.new(42)
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq("DbObject options must be a Hash")
    end
  end

  context "various invalid hash arguments" do
    let(:error_messages) do
      [
        "DbObject options must include a 'name' string",
        "DbObject options for 'foo' must include a 'columns' array"
      ]
    end
    "DbObject options must include a 'name' string, DbObject options for 'unnamed' must include a 'columns' array"
    it "an empty hash does not validate" do
      instance = described_class.new({})
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq(error_messages.values_at(0,1).join(", ").gsub('foo', 'unnamed'))
    end

    it "a name key value only" do
      instance = described_class.new({"name" => "foo"})
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq(error_messages[1])
    end

    it "a name and bad columns" do
      instance = described_class.new({"name" => "foo", "columns" => 42})
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq(error_messages[1])
    end

    it "a name and bad columns - empty array" do
      instance = described_class.new({"name" => "foo", "columns" => []})
      expect(instance.valid?).to be_falsey
      msg = "The columns array for 'foo' is not uniform, it should contain arrays of two strings only"
      expect(instance.formatted_errors).to eq(msg)
    end

    it "a name and bad columns - irregular arrays" do
      instance = described_class.new({"name" => "foo", "columns" => [["ip", "text"], ["name"], ["a", "b", "c"]]})
      expect(instance.valid?).to be_falsey
      msg = "The columns array for 'foo' is not uniform, it should contain arrays of two strings only"
      expect(instance.formatted_errors).to eq(msg)
    end

    it "a name, good columns and bad index_column" do
      instance = described_class.new({"name" => "foo_index", "index_columns" => ["bar"], "columns" => [["ip", "text"], ["name", "text"]]})
      expect(instance.valid?).to be_falsey
      msg = "The index_columns element: 'bar' must be a column defined in the columns array"
      expect(instance.formatted_errors).to eq(msg)
    end
  end

  context "a valid hash argument" do
    it "does validate" do
      instance = described_class.new({"name" => "foo", "index_columns" => ["ip"], "columns" => [["ip", "text"], ["name", "text"]]})
      expect(instance.formatted_errors).to eq("")
      expect(instance.valid?).to be_truthy
    end
  end
end