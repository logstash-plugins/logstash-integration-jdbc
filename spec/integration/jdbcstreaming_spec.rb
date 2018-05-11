require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc_streaming"
require 'jdbc/postgres'
require "sequel"
require "sequel/adapters/jdbc"

module LogStash module Filters
  class TestJdbcStreaming < JdbcStreaming
    attr_reader :database
  end

  describe JdbcStreaming, :integration => true do
    # Use Postgres for integration tests
    ::Jdbc::Postgres.load_driver

    ENV["TZ"] = "Etc/UTC"
    let(:mixin_settings) do
      { "jdbc_user" => "postgres", "jdbc_driver_class" => "org.postgresql.Driver",
        "jdbc_connection_string" => "jdbc:postgresql://localhost/jdbc_streaming_db?user=postgres"}
    end
    let(:settings) { {} }
    let(:plugin) { JdbcStreaming.new(mixin_settings.merge(settings)) }
    let (:db) do
      ::Sequel.connect(mixin_settings['jdbc_connection_string'])
    end
    let(:event)      { ::LogStash::Event.new("message" => "some text", "ip" => ipaddr) }
    let(:cache_expiration) { 3.0 }
    let(:use_cache) { true }
    let(:cache_size) { 10 }
    let(:statement) { "SELECT name, location FROM reference_table WHERE ip = :ip" }
    let(:settings) do
      {
        "statement" => statement,
        "parameters" => {"ip" => "ip"},
        "target" => "server",
        "use_cache" => use_cache,
        "cache_expiration" => cache_expiration,
        "cache_size" => cache_size,
        "tag_on_failure" => ["lookup_failed"],
        "tag_on_default_use" => ["default_used_instead"],
        "default_hash" => {"name" => "unknown", "location" => "unknown"}
      }
    end
    let(:ipaddr)    { "10.#{idx}.1.1" }

    before :each do
      db.create_table :reference_table do
        String :ip
        String  :name
        String  :location
      end
      1.upto(250) do |i|
        db[:reference_table].insert(:ip => "10.#{i}.1.1", :name => "ldn-server-#{i}", :location => "LDN-#{i}-2-3")
      end
      plugin.register
    end

    after(:each) { db.drop_table(:reference_table) }

    describe "found record - uses row" do
      let(:idx) { 200 }

      it "fills in the target" do
        plugin.filter(event)
        expect(event.get("server")).to eq([{"name" => "ldn-server-#{idx}", "location" => "LDN-#{idx}-2-3"}])
        expect((event.get("tags") || []) & ["lookup_failed", "default_used_instead"]).to be_empty
      end
    end

    context "when fetching from cache" do
      let(:plugin) { TestJdbcStreaming.new(mixin_settings.merge(settings)) }
      let(:events) do
        5.times.map{|i| ::LogStash::Event.new("message" => "some other text #{i}", "ip" => ipaddr) }
      end
      let(:call_count) { 1 }
      before(:each) do
        expect(plugin.database).to receive(:[]).exactly(call_count).times.and_call_original
        plugin.filter(event)
      end

      describe "found record - caches row" do
        let(:idx)    { "42" }
        it "calls the database once then uses the cache" do
          expect(event.get("server")).to eq([{"name" => "ldn-server-#{idx}", "location" => "LDN-#{idx}-2-3"}])
          expect(event.get("tags") || []).not_to include("lookup_failed")
          expect(event.get("tags") || []).not_to include("default_used_instead")
          events.each do |evt|
            plugin.filter(evt)
            expect(evt.get("server")).to eq([{"name" => "ldn-server-#{idx}", "location" => "LDN-#{idx}-2-3"}])
          end
        end
      end

      describe "missing record - uses default" do
        let(:idx)    { "252" }
        it "calls the database once then uses the cache" do
          expect(event.get("server")).to eq([{"name" => "unknown", "location" => "unknown"}])
          expect(event.get("tags") & ["lookup_failed", "default_used_instead"]).to eq(["default_used_instead"])
          events.each do |evt|
            plugin.filter(evt)
            expect(evt.get("server")).to eq([{"name" => "unknown", "location" => "unknown"}])
          end
        end
      end

      context "extremely small cache expiration" do
        describe "found record - cache always expires" do
          let(:idx)    { "10" }
          let(:call_count) { 6 }
          let(:cache_expiration) { 0.0000001 }
          it "calls the database each time because cache entry expired" do
            expect(event.get("server")).to eq([{"name" => "ldn-server-#{idx}", "location" => "LDN-#{idx}-2-3"}])
            expect(event.get("tags") || []).not_to include("lookup_failed")
            expect(event.get("tags") || []).not_to include("default_used_instead")
            events.each do |evt|
              plugin.filter(evt)
              expect(evt.get("server")).to eq([{"name" => "ldn-server-#{idx}", "location" => "LDN-#{idx}-2-3"}])
            end
          end
        end
      end

      context "when cache is disabled" do
        let(:call_count) { 6 }
        let(:use_cache) { false }
        describe "database is always called" do
          let(:idx)    { "1" }
          it "calls the database each time" do
            expect(event.get("server")).to eq([{"name" => "ldn-server-#{idx}", "location" => "LDN-#{idx}-2-3"}])
            expect(event.get("tags") || []).not_to include("lookup_failed")
            expect(event.get("tags") || []).not_to include("default_used_instead")
            events.each do |evt|
              plugin.filter(evt)
              expect(evt.get("server")).to eq([{"name" => "ldn-server-#{idx}", "location" => "LDN-#{idx}-2-3"}])
            end
          end
        end

        describe "database is always called but record is missing and default is used" do
          let(:idx)    { "251" }
          it "calls the database each time" do
            expect(event.get("server")).to eq([{"name" => "unknown", "location" => "unknown"}])
            expect(event.get("tags") & ["lookup_failed", "default_used_instead"]).to eq(["default_used_instead"])
            events.each do |evt|
              plugin.filter(evt)
              expect(evt.get("server")).to eq([{"name" => "unknown", "location" => "unknown"}])
            end
          end
        end
      end
    end
  end

end end
