require "logstash/devutils/rspec/spec_helper"
require "logstash/devutils/rspec/shared_examples"
require "logstash/filters/jdbc_streaming"
require "sequel"
require "sequel/adapters/jdbc"

module LogStash module Filters
  class TestJdbcStreaming < JdbcStreaming
    attr_reader :database
  end

  describe JdbcStreaming do
    let!(:jdbc_connection_string) { "jdbc:derby:memory:jdbc_streaming_testdb;create=true"}
    #Use embedded Derby for tests
    #Load Derby driver
    Java::OrgApacheDerbyJdbc::EmbeddedDriver
    ENV["TZ"] = "Etc/UTC"
    describe "plugin level execution" do
      let(:mixin_settings) do
        { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
          "jdbc_connection_string" => jdbc_connection_string}
      end
      let(:plugin) { JdbcStreaming.new(mixin_settings.merge(settings)) }
      let (:db) do
        ::Sequel.connect(mixin_settings['jdbc_connection_string'], :user=> nil, :password=> nil)
      end
      let(:event)      { ::LogStash::Event.new("message" => "some text", "ip" => ipaddr) }
      let(:cache_expiration) { 3.0 }
      let(:use_cache) { true }
      let(:cache_size) { 10 }

      before :each do
        db.create_table :reference_table do
          String  :ip
          String  :name
          String  :location
          Integer :gcode
        end
        db[:reference_table].insert(:ip => "10.1.1.1", :name => "ldn-server-1", :location => "LDN-2-3-4", :gcode => 3)
        db[:reference_table].insert(:ip => "10.2.1.1", :name => "nyc-server-1", :location => "NYC-5-2-8", :gcode => 1)
        db[:reference_table].insert(:ip => "10.3.1.1", :name => "mv-server-1", :location => "MV-9-6-4", :gcode => 1)
        db[:reference_table].insert(:ip => "10.4.1.1", :name => "sf-server-1", :location => "SF-9-5-4", :gcode => 1)
        db[:reference_table].insert(:ip => "10.4.1.1", :name => "mtl-server-1", :location => "MTL-9-3-4", :gcode => 2)
      end

      after :each do
        db.drop_table(:reference_table)
      end

      context "Normal Mode" do
        before :each do
          plugin.register
        end

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

        describe "found record - uses row" do
          let(:ipaddr)    { "10.1.1.1" }

          it "fills in the target" do
            plugin.filter(event)
            expect(event.get("server")).to eq([{"name" => "ldn-server-1", "location" => "LDN-2-3-4"}])
            expect(event.get("tags") || []).not_to include("lookup_failed")
            expect(event.get("tags") || []).not_to include("default_used_instead")
          end
        end

        describe "missing record - uses default" do
          let(:ipaddr)    { "192.168.1.1" }

          it "fills in the target with the default" do
            plugin.filter(event)
            expect(event.get("server")).to eq([{"name" => "unknown", "location" => "unknown"}])
            expect(event.get("tags") & ["lookup_failed", "default_used_instead"]).to eq(["default_used_instead"])
          end
        end

        describe "database error - uses default" do
          let(:ipaddr)    { "10.1.1.1" }
          let(:statement) { "SELECT name, location FROM reference_table WHERE ip = :address" }
          it "fills in the target with the default" do
            plugin.filter(event)
            expect(event.get("server")).to eq([{"name" => "unknown", "location" => "unknown"}])
            expect(event.get("tags") & ["lookup_failed", "default_used_instead"]).to eq(["lookup_failed", "default_used_instead"])
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
            let(:ipaddr)    { "10.1.1.1" }
            it "calls the database once then uses the cache" do
              expect(event.get("server")).to eq([{"name" => "ldn-server-1", "location" => "LDN-2-3-4"}])
              expect(event.get("tags") || []).not_to include("lookup_failed")
              expect(event.get("tags") || []).not_to include("default_used_instead")
              events.each do |evt|
                plugin.filter(evt)
                expect(evt.get("server")).to eq([{"name" => "ldn-server-1", "location" => "LDN-2-3-4"}])
              end
            end
          end

          describe "missing record - uses default" do
            let(:ipaddr)    { "10.10.1.1" }
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
              let(:ipaddr)    { "10.1.1.1" }
              let(:call_count) { 6 }
              let(:cache_expiration) { 0.0000001 }
              it "calls the database each time because cache entry expired" do
                expect(event.get("server")).to eq([{"name" => "ldn-server-1", "location" => "LDN-2-3-4"}])
                expect(event.get("tags") || []).not_to include("lookup_failed")
                expect(event.get("tags") || []).not_to include("default_used_instead")
                events.each do |evt|
                  plugin.filter(evt)
                  expect(evt.get("server")).to eq([{"name" => "ldn-server-1", "location" => "LDN-2-3-4"}])
                end
              end
            end
          end

          context "when cache is disabled" do
            let(:call_count) { 6 }
            let(:use_cache) { false }
            describe "database is always called" do
              let(:ipaddr)    { "10.1.1.1" }
              it "calls the database each time" do
                expect(event.get("server")).to eq([{"name" => "ldn-server-1", "location" => "LDN-2-3-4"}])
                expect(event.get("tags") || []).not_to include("lookup_failed")
                expect(event.get("tags") || []).not_to include("default_used_instead")
                events.each do |evt|
                  plugin.filter(evt)
                  expect(evt.get("server")).to eq([{"name" => "ldn-server-1", "location" => "LDN-2-3-4"}])
                end
              end
            end

            describe "database is always called but record is missing and default is used" do
              let(:ipaddr)    { "10.11.1.1" }
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

      context "Prepared Statement Mode" do
        let(:statement) { "SELECT name, location FROM reference_table WHERE (ip = ?) AND (gcode = ?)" }
        let(:settings) do
          {
            "statement" => statement,
            "use_prepared_statements" => true,
            "prepared_statement_name" => "lookup_ip",
            "prepared_statement_bind_values" => ["[ip]", 2],
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

        describe "using one variable and one constant, found record - uses row" do
          let(:ipaddr)    { "10.4.1.1" }

          it "fills in the target" do
            plugin.register
            expect(plugin.prepared_statement_constant_warned).to be_falsey
            plugin.filter(event)
            expect(event.get("server")).to eq([{"name" => "mtl-server-1", "location" => "MTL-9-3-4"}])
            expect(event.get("tags") || []).not_to include("lookup_failed")
            expect(event.get("tags") || []).not_to include("default_used_instead")
          end
        end

        describe "fails empty name validation" do
          before :each do
            settings["prepared_statement_name"] = ""
          end
          it "should fail to register" do
            expect{ plugin.register }.to raise_error(LogStash::ConfigurationError)
          end
        end

        describe "fails parameter mismatch validation" do
          before :each do
            settings["prepared_statement_bind_values"] = ["[ip]"]
          end
          it "should fail to register" do
            expect{ plugin.register }.to raise_error(LogStash::ConfigurationError)
          end
        end

        describe "warns on constant usage" do
          before :each do
            settings["prepared_statement_bind_values"] = ["ip", 2]
          end
          it "should set the warning logged flag" do
            plugin.register
            expect(plugin.prepared_statement_constant_warned).to be_truthy
          end
        end
      end
    end

    describe "All default - Retrieve a value from database" do
      let(:config) do <<-CONFIG
        filter {
          jdbc_streaming {
            jdbc_driver_class => "org.apache.derby.jdbc.EmbeddedDriver"
            jdbc_connection_string => "#{jdbc_connection_string}"
            statement => "SELECT 'from_database' FROM SYSIBM.SYSDUMMY1"
            target => "new_field"
          }
        }
      CONFIG
      end

      sample({"message" => "some text"}) do
        expect(subject.get('new_field')).to eq([{"1" => 'from_database'}])
      end
    end

    describe "Named column - Retrieve a value from database" do
      let(:config) do <<-CONFIG
        filter {
          jdbc_streaming {
            jdbc_driver_class => "org.apache.derby.jdbc.EmbeddedDriver"
            jdbc_connection_string => "#{jdbc_connection_string}"
            statement => "SELECT 'from_database' as col_1 FROM SYSIBM.SYSDUMMY1"
            target => "new_field"
          }
        }
      CONFIG
      end

      sample({"message" => "some text"}) do
        expect(subject.get('new_field')).to eq([{"col_1" => 'from_database'}])
      end
    end

    describe "Using string parameters - Retrieve a value from database" do
      let(:config) do <<-CONFIG
        filter {
          jdbc_streaming {
            jdbc_driver_class => "org.apache.derby.jdbc.EmbeddedDriver"
            jdbc_connection_string => "#{jdbc_connection_string}"
            statement => "SELECT 'from_database' FROM SYSIBM.SYSDUMMY1 WHERE '1' = :param"
            parameters => { "param" => "param_field"}
            target => "new_field"
          }
        }
      CONFIG
      end

      sample({"message" => "some text", "param_field" => "1"}) do
        expect(subject.get('new_field')).to eq([{"1" => 'from_database'}])
      end

      sample({"message" => "some text", "param_field" => "2"}) do
        expect(subject.get('new_field').nil?)
      end
    end

    describe "Using integer parameters" do
      let(:config) do <<-CONFIG
        filter {
          jdbc_streaming {
            jdbc_driver_class => "org.apache.derby.jdbc.EmbeddedDriver"
            jdbc_connection_string => "#{jdbc_connection_string}"
            statement => "SELECT 'from_database' FROM SYSIBM.SYSDUMMY1 WHERE 1 = :param"
            parameters => { "param" => "param_field"}
            target => "new_field"
          }
        }
      CONFIG
      end

      sample({"message" => "some text", "param_field" => 1}) do
        expect(subject.get('new_field')).to eq([{"1" => 'from_database'}])
      end

      sample({"message" => "some text", "param_field" => "1"}) do
        expect(subject.get('new_field').nil?)
      end
    end

    describe "Using timestamp parameter" do
      let(:config) do <<-CONFIG
        filter {
          jdbc_streaming {
            jdbc_driver_class => "org.apache.derby.jdbc.EmbeddedDriver"
            jdbc_connection_string => "#{jdbc_connection_string}"
            statement => "SELECT 'from_database' FROM SYSIBM.SYSDUMMY1 WHERE {fn TIMESTAMPDIFF( SQL_TSI_DAY, {t :param}, current_timestamp)} = 0"
            parameters => { "param" => "@timestamp"}
            target => "new_field"
          }
        }
      CONFIG
      end

      sample({"message" => "some text"}) do
        expect(subject.get('new_field')).to eq([{"1" => 'from_database'}])
      end
    end

  end
end end
