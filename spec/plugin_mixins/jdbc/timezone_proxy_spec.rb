# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/plugin_mixins/jdbc/timezone_proxy"

describe LogStash::PluginMixins::Jdbc::TimezoneProxy do
  subject(:timezone) { described_class.load(timezone_spec) }

  context 'when handling a daylight-savings ambiguous time' do
    context 'without extensions' do
      let(:timezone_spec) { 'America/Los_Angeles[]' }
      it 'raises an AmbiguousTime error' do
        expect { timezone.local_time(2021,11,7,1,17) }.to raise_error(::TZInfo::AmbiguousTime)
      end
    end
    context 'with extension `dst_enabled_on_overlap:true`' do
      let(:timezone_spec) { 'America/Los_Angeles[dst_enabled_on_overlap:true]' }
      it 'resolves as if DST were enabled' do
        timestamp = timezone.local_time(2021,11,7,1,17)
        aggregate_failures do
          expect(timestamp.dst?).to be true
          expect(timestamp.zone).to eq('PDT') # Pacific Daylight Time
          expect(timestamp.getutc).to eq(Time.utc(2021,11,7,8,17))
          expect(timestamp.utc_offset).to eq( -7 * 3600 )
        end
      end
      end
    context 'with extension `dst_enabled_on_overlap:false`' do
      let(:timezone_spec) { 'America/Los_Angeles[dst_enabled_on_overlap:false]' }
      it 'resolves as if DST were disabled' do
        timestamp = timezone.local_time(2021,11,7,1,17)
        aggregate_failures do
          expect(timestamp.dst?).to be false
          expect(timestamp.zone).to eq('PST') # Pacific Standard Time
          expect(timestamp.getutc).to eq(Time.utc(2021,11,7,9,17))
          expect(timestamp.utc_offset).to eq( -8 * 3600 )
        end
      end
    end
  end

  context '#load' do
    context 'when spec is a normal timezone instance' do
      let(:timezone_spec) { ::TZInfo::Timezone.get('America/Los_Angeles') }
      it 'returns that instance' do
        expect(timezone).to be(timezone_spec)
      end
    end
    context 'when spec is a valid unextended timezone spec' do
      let(:timezone_spec) { 'America/Los_Angeles' }
      it 'returns the canonical timezone' do
        expect(timezone).to eq(::TZInfo::Timezone.get('America/Los_Angeles'))
      end
    end
    context 'when spec is an invalid timezone spec' do
      let(:timezone_spec) { 'NotAValidTimezoneIdentifier' }

      it 'propagates the TZInfo exception' do
        expect { timezone }.to raise_exception(::TZInfo::InvalidTimezoneIdentifier)
      end
    end
    context 'with invalid extension' do
      let(:timezone_spec) { 'America/Los_Angeles[dst_enabled_on_overlap:false;nope:wrong]' }
      it 'raises an exception with a helpful message' do
        expect { timezone }.to raise_exception(ArgumentError, a_string_including("Invalid timezone extension `nope:wrong`"))
      end
    end
  end
end
