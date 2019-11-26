# encoding: utf-8
require "logstash/filters/jdbc/db_object"

RSpec.shared_examples "a single load runner" do

  context "with local db objects" do
    let(:local_db_objects) do
      [
       {"name" => "servers", "index_columns" => ["ip"], "columns" => [%w(ip text), %w(name text), %w(location text)]},
      ]
    end

    it "builds local db objects and populates the local db" do
      expect(local_db).to receive(:populate_all).once.with(loaders)
      expect(local_db).to receive(:build_db_object).once.with(instance_of(LogStash::Filters::Jdbc::DbObject))
      runner.initial_load
      expect(runner.preloaders).to be_a(Array)
      expect(runner.preloaders.size).to eq(1)
      expect(runner.preloaders[0].name).to eq(:servers)
      expect(runner.local).to eq(local_db)
      expect(runner.loaders).to eq(loaders)
    end
  end

  context "without local db objects" do
    it "populates the local db" do
      expect(local_db).to receive(:populate_all).once.with(loaders)
      runner.initial_load
      expect(runner.preloaders).to eq([])
      expect(runner.local).to eq(local_db)
      expect(runner.loaders).to eq(loaders)
    end
  end
end
