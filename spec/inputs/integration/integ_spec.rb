require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/jdbc"
require "sequel"
require "sequel/adapters/jdbc"
require "stud/temporary"
require_relative "security_statements_fixture"


describe LogStash::Inputs::Jdbc, :integration => true do
  # This is a necessary change test-wide to guarantee that no local timezone
  # is picked up.  It could be arbitrarily set to any timezone, but then the test
  # would have to compensate differently.  That's why UTC is chosen.
  ENV["TZ"] = "Etc/UTC"
  # For Travis and CI based on docker, we source from ENV
  jdbc_connection_string = ENV.fetch("PG_CONNECTION_STRING",
                                     "jdbc:postgresql://postgresql:5432") + "/jdbc_input_db?user=postgres"

  OOM_NUM_ROWS = 1_000_000

  let(:settings) do
    { "jdbc_driver_class" => "org.postgresql.Driver",
      "jdbc_connection_string" => jdbc_connection_string,
      "jdbc_driver_library" => "/usr/share/logstash/postgresql.jar",
      "jdbc_user" => "postgres",
      "jdbc_password" => ENV["POSTGRES_PASSWORD"],
      "statement" => 'SELECT FIRST_NAME, LAST_NAME FROM "employee" WHERE EMP_NO = 2'
    }
  end

  let(:plugin) { LogStash::Inputs::Jdbc.new(settings) }
  let(:queue) { Queue.new }

  context "when connecting to a postgres instance" do
    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should populate the event with database entries" do
      plugin.run(queue)
      event = queue.pop
      expect(event.get('first_name')).to eq("Mark")
      expect(event.get('last_name')).to eq("Guckenheimer")
    end

    context 'with paging' do
      let(:settings) do
        super().merge 'jdbc_paging_enabled' => true, 'jdbc_page_size' => 1,
                      "statement" => 'SELECT * FROM "employee" WHERE EMP_NO >= :p1 ORDER BY EMP_NO',
                      'parameters' => { 'p1' => 0 }
      end

      before do # change plugin logger level to debug - to exercise logging
        logger = plugin.class.name.gsub('::', '.').downcase
        logger = org.apache.logging.log4j.LogManager.getLogger(logger)
        @prev_logger_level = [ logger.getName, logger.getLevel ]
        org.apache.logging.log4j.core.config.Configurator.setLevel logger.getName, org.apache.logging.log4j.Level::DEBUG
      end

      after do
        org.apache.logging.log4j.core.config.Configurator.setLevel *@prev_logger_level
      end

      it "should populate the event with database entries" do
        plugin.run(queue)
        event = queue.pop
        expect(event.get('first_name')).to eq('David')
      end
    end

    context 'with temporal columns' do
      let(:settings) do
        super().merge("statement" => 'SELECT ENTRY_DATE, ENTRY_TIME, TIMESTAMP FROM "employee" WHERE EMP_NO = 2')
      end

      before(:each) { plugin.run(queue) }

      subject(:event) { queue.pop }

      it "maps the DATE to a Logstash Timestamp" do
        expect(event.get('entry_date')).to eq(LogStash::Timestamp.new(Time.new(2003, 2, 1)))
      end

      it "maps the TIME field to a Logstash Timestamp" do
        now = DateTime.now
        expect(event.get('entry_time')).to eq(LogStash::Timestamp.new(Time.new(now.year, now.month, now.day, 10, 5, 0)))
      end

      it "maps the TIMESTAMP to a Logstash Timestamp" do
        expect(event.get('timestamp')).to eq(LogStash::Timestamp.new(Time.new(2003, 2, 1, 1, 2, 3)))
      end
    end
  end

  context "when supplying a non-existent library" do
    let(:settings) do
      super().merge(
          "jdbc_driver_library" => "/no/path/to/postgresql.jar"
      )
    end

    it "should not register correctly" do
      expect do
        plugin.register
      end.to raise_error(::LogStash::PluginLoadingError)
    end
  end

  context "when connecting to a non-existent server" do
    let(:settings) do
      super().merge(
          "jdbc_connection_string" => "jdbc:postgresql://localhost:65000/somedb"
      )
    end

    it "log warning msg when plugin run" do
      plugin.register
      expect( plugin ).to receive(:log_java_exception)
      expect(plugin.logger).to receive(:warn).once.with("Exception when executing JDBC query",
                                                        hash_including(:message => instance_of(String)))
      q = Queue.new
      expect{ plugin.run(q) }.not_to raise_error
    end

    it "should log (native) Java driver error" do
      plugin.register
      expect( org.apache.logging.log4j.LogManager ).to receive(:getLogger).and_wrap_original do |m, *args|
        logger = m.call(*args)
        expect( logger ).to receive(:error) do |_, e|
          expect( e ).to be_a org.postgresql.util.PSQLException
        end.and_call_original
        logger
      end
      q = Queue.new
      expect{ plugin.run(q) }.not_to raise_error
    end
  end

  context "when scanning #{OOM_NUM_ROWS} rows via a prepared statement (issue #198)" do
    # Discards event objects so we never materialise OOM_NUM_ROWS Logstash events
    # in heap — only the count matters for this assertion.
    let(:queue) do
      counter = java.util.concurrent.atomic.AtomicLong.new(0)
      q = Object.new
      q.define_singleton_method(:<<) { |_event| counter.increment_and_get }
      q.define_singleton_method(:count) { counter.get }
      q
    end

    let(:settings) do
      {
        "jdbc_driver_class"              => "org.postgresql.Driver",
        "jdbc_connection_string"         => jdbc_connection_string,
        "jdbc_driver_library"            => "/usr/share/logstash/postgresql.jar",
        "jdbc_user"                      => "postgres",
        "jdbc_password"                  => ENV["POSTGRES_PASSWORD"],
        "statement"                      => "SELECT * FROM security_statements",
        "use_prepared_statements"        => true,
        "prepared_statement_name"        => "security_scan_all",
        "prepared_statement_bind_values" => [],
        "last_run_metadata_path"         => Stud::Temporary.pathname
      }
    end

    before(:all) do
      db = Sequel.connect(jdbc_connection_string,
                          :user => "postgres", :password => ENV["POSTGRES_PASSWORD"])
      SecurityStatementsFixture.populate(db, OOM_NUM_ROWS)
      db.disconnect
    end

    after(:all) do
      db = Sequel.connect(jdbc_connection_string,
                          :user => "postgres", :password => ENV["POSTGRES_PASSWORD"])
      SecurityStatementsFixture.clear_table(db)
      db.disconnect
    end

    after(:each) do
      plugin.stop rescue nil
    end

    it "reads all #{OOM_NUM_ROWS} rows exactly once without materialising the full result set" do
      plugin.register
      plugin.run(queue)

      expect(queue.count).to eq(OOM_NUM_ROWS)
    end
  end
end

