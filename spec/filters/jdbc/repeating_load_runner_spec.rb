# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require_relative "../shared_helpers"
require "logstash/filters/jdbc/repeating_load_runner"

module LogStash module Filters module Jdbc
  describe RepeatingLoadRunner  do
    let(:local_db) { double("local_db") }
    let(:loaders) { Object.new }
    let(:local_db_objects) { [] }
    subject(:runner) { described_class.new(local_db, loaders, local_db_objects) }

    it_behaves_like "a single load runner"

    context "when repeating" do
      it "repopulates the local db" do
        expect(local_db).to receive(:populate_all).once.with(loaders)
        expect(local_db).to receive(:repopulate_all).once.with(loaders)
        runner.initial_load
        subject.repeated_load
      end
    end
  end
end end end
