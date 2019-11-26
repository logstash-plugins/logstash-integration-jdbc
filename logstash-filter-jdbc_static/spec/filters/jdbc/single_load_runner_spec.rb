# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require_relative "../shared_helpers"

require "logstash/filters/jdbc/single_load_runner"

module LogStash module Filters module Jdbc
  describe SingleLoadRunner  do
    let(:local_db) { double("local_db") }
    let(:loaders) { Object.new }
    let(:local_db_objects) { [] }
    subject(:runner) { described_class.new(local_db, loaders, local_db_objects) }

    it_behaves_like "a single load runner"
  end
end end end
