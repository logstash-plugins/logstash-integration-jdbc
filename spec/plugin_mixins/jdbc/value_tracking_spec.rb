# encoding: utf-8
require "logstash/plugin_mixins/jdbc/value_tracking"

module LogStash module PluginMixins module Jdbc
  describe ValueTracking do

    let(:yaml_date_source) { "--- !ruby/object:DateTime '2023-06-15 09:59:30.558000000 +02:00'\n" }

    context "#load_yaml" do
      it "should load yaml with date string" do
        parsed_date = LogStash::PluginMixins::Jdbc::ValueTracking.load_yaml(yaml_date_source)
        expect(parsed_date.year).to eq 2023
        expect(parsed_date.month).to eq 6
        expect(parsed_date.day).to eq 15
      end
    end
  end
end end end
