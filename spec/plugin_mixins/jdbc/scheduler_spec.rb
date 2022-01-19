# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/plugin_mixins/jdbc/scheduler"

describe LogStash::PluginMixins::Jdbc::Scheduler do

  let(:thread_name) { '[test]<jdbc_scheduler' }

  let(:opts) do
    { :max_work_threads => 2, :thread_name => thread_name }
  end

  subject(:scheduler) { LogStash::PluginMixins::Jdbc::Scheduler.new(opts) }

  after { scheduler.stop(:wait) }

  it "sets scheduler thread name" do
    expect( scheduler.thread.name ).to include thread_name
  end

  context 'cron schedule' do

    before do
      scheduler.schedule_cron('* * * * * *') { sleep 1.25 } # every second
    end

    it "sets worker thread names" do
      sleep 3.0
      threads = scheduler.work_threads
      threads.sort! { |t1, t2| (t1.name || '') <=> (t2.name || '') }

      expect( threads.size ).to eql 2
      expect( threads.first.name ).to eql "#{thread_name}_worker-00"
      expect( threads.last.name ).to eql "#{thread_name}_worker-01"
    end

  end

  context 'every 1s' do

    before do
      scheduler.schedule_in('1s') { raise 'TEST' } # every second
    end

    it "logs errors handled" do
      expect( scheduler.logger ).to receive(:error).with /Scheduler intercepted an error/, hash_including(:message => 'TEST')
      sleep 1.5
    end

  end

end