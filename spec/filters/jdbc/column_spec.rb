# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc/column"

describe LogStash::Filters::Jdbc::Column do
  let(:invalid_messages) do
    [
      "The column options must be an array",
      "The first column option is the name and must be a string",
      "The second column option is the datatype and must be a string"
    ]
  end

  context "various invalid non-array arguments" do
    it "a nil does not validate" do
      instance = described_class.new(nil)
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq(invalid_messages.join(", "))
    end

    it "a string does not validate" do
      instance = described_class.new("foo")
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq(invalid_messages.values_at(0,2).join(", "))
    end

    it "a number does not validate" do
      instance = described_class.new(42)
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq(invalid_messages.join(", "))
    end
  end

  context "various invalid array arguments" do
    it "a single string element does not validate" do
      instance = described_class.new(["foo"])
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq(invalid_messages.last)
    end
    [ [], [1, 2] ].each do |arg|
      it "do not validate" do
        instance = described_class.new(arg)
        expect(instance.valid?).to be_falsey
        expect(instance.formatted_errors).to eq(invalid_messages.values_at(1,2).join(", "))
      end
    end
    [ ["foo", 3], ["foo", nil] ].each do |arg|
      it "do not validate" do
        instance = described_class.new(arg)
        expect(instance.valid?).to be_falsey
        expect(instance.formatted_errors).to eq(invalid_messages.last)
      end
    end
    [ [3, "foo"], [nil, "foo"] ].each do |arg|
      it "do not validate" do
        instance = described_class.new(arg)
        expect(instance.valid?).to be_falsey
        expect(instance.formatted_errors).to eq(invalid_messages[1])
      end
    end
  end

  context "a valid array argument" do
    it "does validate" do
      instance = described_class.new(["foo", "varchar2"])
      expect(instance.valid?).to be_truthy
      expect(instance.formatted_errors).to eq("")
    end
  end
end
