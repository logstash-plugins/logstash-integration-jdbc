require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc_streaming"
require "sequel"
require "sequel/adapters/jdbc"

module LogStash module Filters
  class TestJdbcStreaming < JdbcStreaming
    attr_reader :database
  end

  describe JdbcStreaming, :integration => true do
    ENV["TZ"] = "Etc/UTC"

    # For Travis and CI based on docker, we source from ENV
    jdbc_connection_string = ENV.fetch("PG_CONNECTION_STRING",
      "jdbc:postgresql://postgresql:5432") + "/jdbc_streaming_db?user=postgres"

    let(:mixin_settings) do
      {
        "jdbc_user" => ENV['USER'],
        "jdbc_password" => ENV["POSTGRES_PASSWORD"],
        "jdbc_driver_class" => "org.postgresql.Driver",
        "jdbc_driver_library" => "/usr/share/logstash/postgresql.jar",
        "jdbc_connection_string" => jdbc_connection_string
      }
    end
    let(:plugin) { JdbcStreaming.new(mixin_settings.merge(settings)) }
    let(:db) do
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
        "default_hash" => {"name" => "unknown", "location" => "unknown"},
        "sequel_opts" => {"pool_timeout" => 600}
      }
    end
    let(:ipaddr)    { "10.#{idx}.1.1" }

    before :each do
      plugin.register
    end

    describe "found record - uses row" do
      let(:idx) { 200 }

      it "fills in the target" do
        plugin.filter(event)
        expect(event.get("server")).to eq([{"name" => "ldn-server-#{idx}", "location" => "LDN-#{idx}-2-3"}])
        expect((event.get("tags") || []) & ["lookup_failed", "default_used_instead"]).to be_empty
      end
    end

    describe "In Prepared Statement mode, found record - uses row" do
      let(:idx) { 200 }
      let(:statement) { "SELECT name, location FROM reference_table WHERE ip = ?" }
      let(:settings) do
        {
          "statement" => statement,
          "use_prepared_statements" => true,
          "prepared_statement_name" => "lookup_ip",
          "prepared_statement_bind_values" => ["[ip]"],
          "target" => "server",
          "use_cache" => use_cache,
          "cache_expiration" => cache_expiration,
          "cache_size" => cache_size,
          "tag_on_failure" => ["lookup_failed"],
          "tag_on_default_use" => ["default_used_instead"],
          "default_hash" => {"name" => "unknown", "location" => "unknown"},
          "sequel_opts" => {"pool_timeout" => 600}
        }
      end
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
