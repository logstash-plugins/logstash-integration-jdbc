# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/plugin_mixins/jdbc_streaming/parameter_handler"


describe LogStash::PluginMixins::JdbcStreaming::ParameterHandler do
  context "resolve field reference" do
    let(:event) { ::LogStash::Event.new("field" => "field_value") }

    it "should resolve root field" do
      handler = LogStash::PluginMixins::JdbcStreaming::ParameterHandler.build_bind_value_handler "[field]"
      handler.extract_from(event)
      expect(handler.extract_from(event)).to eq "field_value"
    end

    it "should resolve nested field" do
      event = ::LogStash::Event.from_json("{\"field\": {\"nested\": \"nested_field\"}}").first
      handler = LogStash::PluginMixins::JdbcStreaming::ParameterHandler.build_bind_value_handler "[field][nested]"
      handler.extract_from(event)
      expect(handler.extract_from(event)).to eq "nested_field"
    end
  end
end