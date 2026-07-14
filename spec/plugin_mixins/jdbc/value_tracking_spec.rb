# encoding: utf-8
require "logstash/plugin_mixins/jdbc/value_tracking"
require "tempfile"

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

    context "#build_last_value_tracker" do

      let(:plugin) { double("fake plugin") }
      let(:temp_file) { Tempfile.new('last_run_tracker') }

      before(:each) do
        allow(plugin).to receive(:record_last_run).and_return(true)
        allow(plugin).to receive(:clean_run).and_return(false)
        allow(plugin).to receive(:last_run_metadata_file_path).and_return(temp_file.path)
      end

      context "create numerical tracker" do
        before(:each) do
          allow(plugin).to receive(:use_column_value).and_return(true)
          allow(plugin).to receive(:tracking_column_type).and_return("numeric")
        end

        it "should write correctly" do
          tracker = ValueTracking.build_last_value_tracker(plugin)
          tracker.set_value(1)
          tracker.write

          temp_file.rewind
          v = ValueTracking.load_yaml(::File.read(temp_file.path))
          expect(v).to eq 1
        end
      end

      context "create date time tracker" do
        before(:each) do
          allow(plugin).to receive(:use_column_value).and_return(false)
          allow(plugin).to receive(:jdbc_default_timezone).and_return(:something_not_nil)
        end

        it "should write correctly" do
          tracker = ValueTracking.build_last_value_tracker(plugin)
          tracker.set_value("2023-06-15T15:28:15+02:00")
          tracker.write

          temp_file.rewind
          v = ValueTracking.load_yaml(::File.read(temp_file.path))
          expect(v.class).to eq DateTime
          expect(v.year).to eq 2023
        end
      end

      context "create time tracker" do
        before(:each) do
          allow(plugin).to receive(:use_column_value).and_return(false)
          allow(plugin).to receive(:jdbc_default_timezone).and_return(nil)
        end

        it "should write correctly" do
          tracker = ValueTracking.build_last_value_tracker(plugin)
          tracker.set_value("2023-06-15T15:28:15+02:00")
          tracker.write

          temp_file.rewind
          v = ValueTracking.load_yaml(::File.read(temp_file.path))
          expect(v.class).to eq Time
          expect(v.min).to eq 28
        end
      end

    end

    context "FileHandler atomic write" do
      let(:temp_file) { Tempfile.new('last_run_tracker') }
      let(:path) { temp_file.path }
      let(:handler) { ValueTracking::FileHandler.new(path) }

      after(:each) do
        temp_file.close
        temp_file.unlink
      end

      it "should write value correctly via temp file + rename" do
        handler.write(42)
        expect(::File.read(path)).to eq YAML.dump(42)
      end

      it "should not leave a temp file after successful write" do
        handler.write(42)
        tmp_files = Dir.glob("#{path}.tmp.*")
        expect(tmp_files).to be_empty
      end

      it "should preserve existing file when temp write fails" do
        # seed with a valid value
        ::File.write(path, YAML.dump(99))

        # simulate failure when writing the temp file
        allow(::File).to receive(:write).and_call_original
        allow(::File).to receive(:write).with(/\.tmp\.\d+$/, anything).and_raise(Errno::ENOSPC)

        expect { handler.write(100) }.to raise_error(Errno::ENOSPC)

        # original file must be untouched
        expect(::File.read(path)).to eq YAML.dump(99)
        # no temp file residue
        expect(Dir.glob("#{path}.tmp.*")).to be_empty
      end

      it "should keep old content intact until rename succeeds" do
        ::File.write(path, YAML.dump(1))

        # intercept rename to verify target still has old content at that moment
        old_content_during_rename = nil
        allow(::File).to receive(:rename).and_wrap_original do |original, from, to|
          old_content_during_rename = ::File.read(to)
          original.call(from, to)
        end

        handler.write(2)

        expect(old_content_during_rename).to eq YAML.dump(1)
        expect(::File.read(path)).to eq YAML.dump(2)
      end
    end
  end
end end end
