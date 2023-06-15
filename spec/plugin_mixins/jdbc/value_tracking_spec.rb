# encoding: utf-8
require "logstash/plugin_mixins/jdbc/value_tracking"

module LogStash module PluginMixins module Jdbc
  describe ValueTracking do
    context "#load_yaml" do

      context "with date string" do
        let(:yaml_date_source) { "--- !ruby/object:DateTime '2023-06-15 09:59:30.558000000 +02:00'\n" }

        it "should be loaded" do
          parsed_date = LogStash::PluginMixins::Jdbc::ValueTracking.load_yaml(yaml_date_source)
          expect(parsed_date.class).to eq DateTime
          expect(parsed_date.year).to eq 2023
          expect(parsed_date.month).to eq 6
          expect(parsed_date.day).to eq 15
        end
      end

      context "with time string" do
        let(:yaml_time_source) { "--- 2023-06-15 15:28:15.227874000 +02:00\n" }

        it "should be loaded" do
          parsed_time = LogStash::PluginMixins::Jdbc::ValueTracking.load_yaml(yaml_time_source)
          expect(parsed_time.class).to eq Time
          expect(parsed_time.year).to eq 2023
          expect(parsed_time.month).to eq 6
          expect(parsed_time.day).to eq 15
          expect(parsed_time.hour).to eq 15
          expect(parsed_time.min).to eq 28
          expect(parsed_time.sec).to eq 15
        end
      end

      context "with date string" do
        let(:yaml_bigdecimal_source) { "--- !ruby/object:BigDecimal '0:0.1e1'\n" }

        it "should be loaded" do
          parsed_bigdecimal = LogStash::PluginMixins::Jdbc::ValueTracking.load_yaml(yaml_bigdecimal_source)
          expect(parsed_bigdecimal.class).to eq BigDecimal
          expect(parsed_bigdecimal.to_i).to eq 1
        end
      end
    end
  end
end end end
