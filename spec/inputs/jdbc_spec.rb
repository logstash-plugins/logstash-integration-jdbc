# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/devutils/rspec/shared_examples"
require "logstash/inputs/jdbc"
require "sequel"
require "sequel/adapters/jdbc"
require "timecop"
require "stud/temporary"
require "time"
require "date"
require "pathname"

# We do not need to set TZ env var anymore because we can have 'Sequel.application_timezone' set to utc by default now.

describe LogStash::Inputs::Jdbc do
  let(:connection_string) { "jdbc:derby:memory:testdb;create=true" }
  let(:jdbc_driver_class) { "org.apache.derby.jdbc.EmbeddedDriver" }
  let(:mixin_settings) do
    {
        "jdbc_user" => ENV['USER'],
        "jdbc_driver_class" => jdbc_driver_class,
        "jdbc_connection_string" => connection_string
    }
  end
  let(:settings) { {} }
  let(:plugin) { LogStash::Inputs::Jdbc.new(mixin_settings.merge(settings)) }
  let(:queue) { Queue.new }
  let (:db) do
    Sequel.connect(mixin_settings['jdbc_connection_string'], :user=> nil, :password=> nil)
  end

  before :each do
    if !RSpec.current_example.metadata[:no_connection]
      # before body
      db.create_table :test_table do
        DateTime     :created_at
        BigDecimal   :big_num
        String       :uuid
        Integer      :num
        String       :string
        DateTime     :custom_time
      end
      db << "CREATE TABLE types_table (num INTEGER, string VARCHAR(255), started_at DATE, custom_time TIMESTAMP, ranking DECIMAL(16,6), uuid VARCHAR(36))"
      db << "CREATE TABLE test1_table (num INTEGER, string VARCHAR(255), custom_time TIMESTAMP, created_at TIMESTAMP)"
    end
  end

  after :each do
    if !RSpec.current_example.metadata[:no_connection]
      db.drop_table(:test_table)
      db.drop_table(:types_table)
      db.drop_table(:test1_table)
    end

    last_run_default_path = LogStash::SETTINGS.get_value("path.data")
    FileUtils.rm_f("#{last_run_default_path}/plugins/inputs/jdbc/logstash_jdbc_last_run")
  end

  context "when registering and tearing down" do
    let(:settings) { {"statement" => "SELECT 1 as col1 FROM test_table"} }

    it "should register without raising exception" do
      expect { plugin.register }.to_not raise_error
      plugin.stop
    end

    it "should register with password set" do
      mixin_settings['jdbc_password'] = 'pass'
      expect { plugin.register }.to_not raise_error
      plugin.stop
    end

    it "should stop without raising exception" do
      plugin.register
      expect { plugin.stop }.to_not raise_error
    end

    it_behaves_like "an interruptible input plugin" do
      let(:settings) do
        {
          "statement" => "SELECT 1 FROM test_table",
          "schedule" => "* * * * * UTC"
        }
      end
      let(:config) { mixin_settings.merge(settings) }
    end
  end

  context "when both jdbc_password and jdbc_password_filepath arguments are passed" do
    let(:statement) { "SELECT * from test_table" }
    let(:jdbc_password) { "secret" }
    let(:jdbc_password_file_path) { Stud::Temporary.pathname }
    let(:settings) { { "jdbc_password_filepath" => jdbc_password_file_path,
                       "jdbc_password" => jdbc_password,
                       "statement" => statement } }

    it "should fail to register" do
      expect{ plugin.register }.to raise_error(LogStash::ConfigurationError)
    end
  end

  context "when jdbc_password is passed in from a file" do
    let(:statement) { "SELECT * from test_table" }
    let(:jdbc_password) { "secret" }
    let(:jdbc_password_file_path) { Stud::Temporary.pathname }
    let(:settings) { { "jdbc_password_filepath" => jdbc_password_file_path,
                       "statement" => statement } }

    before do
      File.write(jdbc_password_file_path, jdbc_password)
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should read in jdbc_password from file" do
      expect(plugin.jdbc_password.value).to eq(jdbc_password)
    end
  end

  context "when sequel opts has user option" do
    let(:settings) do
      {
          "jdbc_user" => 'system',
          "statement" => "SELECT * from test_table",
          "sequel_opts" => { "user" => 'from-opts' }
      }
    end

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should honor set jdbc-user when connecting" do
      expect( Sequel ).to receive(:connect).with connection_string, hash_including(:user=>"system")
      plugin.send(:jdbc_connect)
    end
  end

  context "with sequel opts" do
    let(:settings) do
      {
          "jdbc_user" => 'system',
          "statement" => "SELECT * from test_table",
          "sequel_opts" => {
              "truthy" => 'true',
              "falsey" => 'false',
              "foo" => 'bar',
              "jdbc_properties" => { "some" => 'true' }
          }
      }
    end

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should symbolize keys" do
      expect( Sequel ).to receive(:connect).with connection_string,
          hash_including(:truthy => true, :falsey => false, :foo => 'bar', :jdbc_properties => { 'some' => 'true' })
      plugin.send(:jdbc_connect)
    end
  end

  context "when neither statement and statement_filepath arguments are passed" do
    it "should fail to register" do
      expect{ plugin.register }.to raise_error(LogStash::ConfigurationError)
    end
  end

  context "when both statement and statement_filepath arguments are passed" do
    let(:statement) { "SELECT * from test_table" }
    let(:statement_file_path) { Stud::Temporary.pathname }
    let(:settings) { { "statement_filepath" => statement_file_path, "statement" => statement } }

    it "should fail to register" do
      expect{ plugin.register }.to raise_error(LogStash::ConfigurationError)
    end
  end

  context "when statement is passed in from a file" do
    let(:statement) { "SELECT * from test_table" }
    let(:statement_file_path) { Stud::Temporary.pathname }
    let(:settings) { { "statement_filepath" => statement_file_path } }

    before do
      File.write(statement_file_path, statement)
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should read in statement from file" do
      expect(plugin.statement).to eq(statement)
    end
  end

  context "when passing parameters" do
    let(:settings) do
      {
        "statement" => "SELECT :num_param as num_param FROM SYSIBM.SYSDUMMY1",
        "parameters" => { "num_param" => 10}
      }
    end

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should retrieve params correctly from Event" do
      plugin.run(queue)
      expect(queue.pop.get('num_param')).to eq(settings['parameters']['num_param'])
    end
  end

  context "when scheduling" do
    let(:settings) { {"statement" => "SELECT 1 as num_param FROM SYSIBM.SYSDUMMY1", "schedule" => "* * * * * UTC"} }

    before do
      plugin.register
    end

    it "should properly schedule" do
      Timecop.travel(Time.new(2000))
      Timecop.scale(60)
      runner = Thread.new do
        plugin.run(queue)
      end
      sleep 3
      plugin.stop
      runner.kill
      runner.join
      expect(queue.size).to eq(2)
      Timecop.return
    end

  end

  context "when scheduling and previous runs are to be preserved" do
    let(:settings) do
      {
        "statement" => "SELECT 1 as num_param FROM SYSIBM.SYSDUMMY1",
        "schedule" => "* * * * * UTC",
        "last_run_metadata_path" => Stud::Temporary.pathname
      }
    end

    let(:last_run_time) { Time.at(1).utc }

    before do
      plugin.register
    end

    it "should flush previous run metadata per query execution" do
      Timecop.travel(Time.new(2000))
      Timecop.scale(60)
      runner = Thread.new do
        plugin.run(queue)
      end
      sleep 1
      for i in 0..1
        sleep 1
        updated_last_run = LogStash::PluginMixins::Jdbc::ValueTracking.load_yaml(File.read(settings["last_run_metadata_path"]))
        expect(updated_last_run).to be > last_run_time
        last_run_time = updated_last_run
      end

      plugin.stop
      runner.join
      Timecop.return
    end

  end

  context "when iterating result-set via paging" do

    let(:settings) do
      {
        "statement" => "SELECT * from test_table",
        "jdbc_paging_enabled" => true,
        "jdbc_page_size" => 20
      }
    end

    let(:num_rows) { 1000 }

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should fetch all rows" do
      num_rows.times do
        db[:test_table].insert(:num => 1, :custom_time => Time.now.utc, :created_at => Time.now.utc)
      end

      plugin.run(queue)

      expect(queue.size).to eq(num_rows)
    end

  end

  context "when iterating result-set via explicit paging mode" do

    let(:settings) do
      {
        "statement" => "SELECT * from test_table OFFSET :offset ROWS FETCH NEXT :size ROWS ONLY",
        "jdbc_paging_enabled" => true,
        "jdbc_paging_mode" => "explicit",
        "jdbc_page_size" => 10
      }
    end

    let(:num_rows) { 15 }

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should fetch all rows" do
      num_rows.times do
        db[:test_table].insert(:num => 1, :custom_time => Time.now.utc, :created_at => Time.now.utc)
      end

      plugin.run(queue)

      expect(queue.size).to eq(num_rows)
    end

  end

  context "when using target option" do
    let(:settings) do
      {
        "statement" => "SELECT * from test_table FETCH FIRST 1 ROWS ONLY",
        "target" => "sub_field"
      }
    end

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should put all columns under sub-field" do
      db[:test_table].insert(:num => 1, :custom_time => Time.now.utc, :created_at => Time.now.utc, :string => "Test target option")

      plugin.run(queue)

      expect(queue.size).to eq(1)
      event = queue.pop
      expect(event.get("[sub_field][string]")).to eq("Test target option")
    end
  end

  context "when using target option is not set and ecs_compatibility is enabled" do
    let(:settings) do
      {
        "statement" => "SELECT * from test_table FETCH FIRST 1 ROWS ONLY",
        "ecs_compatibility" => :v1
      }
    end

    it "should log a warn of missed target usage" do
      expect(plugin.logger).to receive(:info).once.with(a_string_including("ECS compatibility is enabled").and(including("target")))

      plugin.register
    end
  end

  context "when fetching time data" do

    let(:settings) do
      {
        "statement" => "SELECT * from test_table",
      }
    end

    let(:num_rows) { 10 }

    before do
      num_rows.times do
        db[:test_table].insert(:num => 1, :custom_time => Time.now.utc, :created_at => Time.now.utc)
      end

      plugin.register
    end

    after do
      plugin.stop
    end

    it "should convert it to LogStash::Timestamp " do
      plugin.run(queue)
      event = queue.pop
      expect(event.get("custom_time")).to be_a(LogStash::Timestamp)
    end
  end

  describe "when jdbc_default_timezone is set" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true",
        "jdbc_default_timezone" => "America/Chicago"
      }
    end

    let(:hours) { [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20] }

    context "when fetching time data and the tracking column is set and tracking column type defaults to 'numeric'" do
      let(:settings) do
        {
          "statement" => "SELECT * from test_table WHERE num > :sql_last_value",
          "last_run_metadata_path" => Stud::Temporary.pathname,
          "tracking_column" => "num",
          "use_column_value" => true
        }
      end

      it "should convert the time to reflect the timezone " do
        File.write(settings["last_run_metadata_path"], YAML.dump(42))

        db[:test_table].insert(:num => 42, :custom_time => "2015-01-01 10:10:10", :created_at => Time.now.utc)
        db[:test_table].insert(:num => 43, :custom_time => "2015-01-01 11:11:11", :created_at => Time.now.utc)

        plugin.register
        plugin.run(queue)
        plugin.stop
        expect(queue.size).to eq(1)
        event = queue.pop
        expect(event.get("num")).to eq(43)
        expect(event.get("custom_time").time).to eq(Time.iso8601("2015-01-01T17:11:11.000Z"))
      end
    end

    context "when fetching time data and the tracking column is NOT set, sql_last_value is time of run" do

      let(:settings) do
        {
          "statement" => "SELECT * from test_table WHERE custom_time > :sql_last_value",
          "last_run_metadata_path" => Stud::Temporary.pathname
        }
      end

      before do
        last_run_value = DateTime.iso8601("2000-01-01T12:00:00.000Z")
        File.write(settings["last_run_metadata_path"], last_run_value)
        Timecop.travel(DateTime.iso8601("2015-01-01T15:50:01.000Z")) do
          # simulate earlier records written
          hours.each do |i|
            db[:test_table].insert(:num => i, :custom_time => "2015-01-01 #{i}:00:00", :created_at => Time.now.utc)
          end
        end
      end

      it "should convert the time to reflect the timezone " do
        Timecop.travel(DateTime.iso8601("2015-01-02T02:10:00.000Z")) do
          # simulate the first plugin run after the custom time of the last record
          plugin.register
          plugin.run(queue)
          expected = hours.map{|hour| Time.iso8601("2015-01-01T06:00:00.000Z") + (hour * 3600) }# because Sequel converts the column values to Time instances.
          actual = queue.size.times.map { queue.pop.get("custom_time").time }
          expect(actual).to eq(expected)
          plugin.stop
        end
        Timecop.travel(DateTime.iso8601("2015-01-02T02:20:00.000Z")) do
          # simulate a run 10 minutes later
          plugin.register
          plugin.run(queue)
          expect(queue.size).to eq(0) # no new records
          plugin.stop
          # now add records
          db[:test_table].insert(:num => 11, :custom_time => "2015-01-01 20:20:20", :created_at => Time.now.utc)
          db[:test_table].insert(:num => 12, :custom_time => "2015-01-01 21:21:21", :created_at => Time.now.utc)
        end
        Timecop.travel(DateTime.iso8601("2015-01-02T03:30:00.000Z")) do
          # simulate another run later than the custom time of the last record
          plugin.register
          plugin.run(queue)
          expect(queue.size).to eq(2)
          plugin.stop
        end
        event = queue.pop
        expect(event.get("num")).to eq(11)
        expect(event.get("custom_time").time).to eq(Time.iso8601("2015-01-02T02:20:20.000Z"))
        event = queue.pop
        expect(event.get("num")).to eq(12)
        expect(event.get("custom_time").time).to eq(Time.iso8601("2015-01-02T03:21:21.000Z"))
      end
    end

    context "when fetching time data and the tracking column is set, sql_last_value is sourced from a column, sub-second precision is maintained" do
      let(:settings) do
        {
          "statement" => "SELECT * from test1_table WHERE custom_time > :sql_last_value ORDER BY custom_time",
          "use_column_value" => true,
          "tracking_column" => "custom_time",
          "tracking_column_type" => "timestamp",
          "last_run_metadata_path" => Stud::Temporary.pathname
        }
      end

      let(:msecs) { [111, 122, 233, 244, 355, 366, 477, 488, 599, 611, 722] }

      it "should convert the time to reflect the timezone " do
        # Sequel only does the *correct* timezone calc on a DateTime instance
        last_run_value = DateTime.iso8601("2000-01-01T00:00:00.987Z")
        File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))
        hours.each_with_index do |i, j|
          time_value = Time.utc(2015, 1, 1, i, 0, 0, msecs[j] * 1000)
          db[:test1_table].insert(:num => i, :custom_time => time_value, :created_at => Time.now.utc)
        end

        plugin.register

        plugin.run(queue)
        expected = hours.map.with_index {|hour, i| Time.iso8601("2015-01-01T06:00:00.000Z") + (hour * 3600 + (msecs[i] / 1000.0)) }
        actual = queue.size.times.map { queue.pop.get("custom_time").time }
        expect(actual).to eq(expected)
        plugin.stop
        raw_last_run_value = File.read(settings["last_run_metadata_path"])
        last_run_value = LogStash::PluginMixins::Jdbc::ValueTracking.load_yaml(raw_last_run_value)
        expect(last_run_value).to be_a(DateTime)
        expect(last_run_value.strftime("%F %T.%N %Z")).to eq("2015-01-02 02:00:00.722000000 +00:00")

        plugin.run(queue)
        plugin.stop
        db[:test1_table].insert(:num => 11, :custom_time => "2015-01-01 11:00:00.099", :created_at => Time.now.utc)
        db[:test1_table].insert(:num => 12, :custom_time => "2015-01-01 21:00:00.811", :created_at => Time.now.utc)
        expect(queue.size).to eq(0)
        plugin.run(queue)
        expect(queue.size).to eq(1)
        event = queue.pop
        plugin.stop
        expect(event.get("num")).to eq(12)
        expect(event.get("custom_time").time).to eq(Time.iso8601("2015-01-02T03:00:00.811Z"))
        last_run_value = LogStash::PluginMixins::Jdbc::ValueTracking.load_yaml(File.read(settings["last_run_metadata_path"]))
        expect(last_run_value).to be_a(DateTime)
        # verify that sub-seconds are recorded to the file
        expect(last_run_value.strftime("%F %T.%N %Z")).to eq("2015-01-02 03:00:00.811000000 +00:00")
      end
    end
  end

  context "when fetching time data without jdbc_default_timezone set" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"
      }
    end

    let(:settings) do
      {
        "statement" => "SELECT * from test_table",
      }
    end

    let(:num_rows) { 1 }

    before do
      num_rows.times do
        db.run "INSERT INTO test_table (created_at, num, custom_time) VALUES (TIMESTAMP('2015-01-01 12:00:00'), 1, TIMESTAMP('2015-01-01 12:00:00'))"
      end

      plugin.register
    end

    after do
      plugin.stop
    end

    it "should not convert the time to reflect the timezone " do
      plugin.run(queue)
      event = queue.pop
      # With no timezone set, no change should occur
      expect(event.get("custom_time").time).to eq(Time.iso8601("2015-01-01T12:00:00Z"))
    end
  end

  context "when iteratively running plugin#run" do
    let(:settings) do
      {"statement" => "SELECT num, created_at FROM test_table WHERE created_at > :sql_last_value"}
    end

    let(:nums) { [10, 20, 30, 40, 50] }

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should successfully iterate table with respect to field values" do
      test_table = db[:test_table]

      plugin.run(queue)
      test_table.insert(:num => nums[0], :created_at => Time.now.utc)
      test_table.insert(:num => nums[1], :created_at => Time.now.utc)
      plugin.run(queue)
      test_table.insert(:num => nums[2], :created_at => Time.now.utc)
      test_table.insert(:num => nums[3], :created_at => Time.now.utc)
      test_table.insert(:num => nums[4], :created_at => Time.now.utc)
      plugin.run(queue)

      actual_sum = 0
      until queue.empty? do
        actual_sum += queue.pop.get('num')
      end

      expect(actual_sum).to eq(nums.inject{|sum,x| sum + x })
    end
  end

  context "when iteratively running plugin#run with tracking_column" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"
      }
    end

    let(:settings) do
      { "statement" => "SELECT num, created_at FROM test_table WHERE num > :sql_last_value",
        "use_column_value" => true,
        "tracking_column" => "num",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:nums) { [10, 20, 30, 40, 50] }

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should successfully update sql_last_value" do
      test_table = db[:test_table]

      plugin.run(queue)
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(0)
      test_table.insert(:num => nums[0], :created_at => Time.now.utc)
      test_table.insert(:num => nums[1], :created_at => Time.now.utc)
      plugin.run(queue)
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(20)
      test_table.insert(:num => nums[2], :created_at => Time.now.utc)
      test_table.insert(:num => nums[3], :created_at => Time.now.utc)
      test_table.insert(:num => nums[4], :created_at => Time.now.utc)
      plugin.run(queue)
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(50)
    end
  end

  context "Test Sql Last Value with a BigDecimal `sqlLastValue`" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"
      }
    end

    let(:settings) do
      { "statement" => "SELECT big_num, created_at FROM test_table WHERE big_num > :sql_last_value",
        "use_column_value" => true,
        "tracking_column" => "big_num",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:nums) { [BigDecimal(10), BigDecimal(20), BigDecimal(30), BigDecimal(40), BigDecimal(50)] }

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should only pick up new values" do
      test_table = db[:test_table]

      plugin.run(queue)
      expect(queue.size).to eq(0)
      test_table.insert(:big_num => nums[0], :created_at => Time.now.utc)
      test_table.insert(:big_num => nums[1], :created_at => Time.now.utc)
      queue = []
      # Stop and restart the plugin to have it read saved value of `sql_last_value`
      plugin.stop
      plugin.register
      plugin.run(queue)
      expect(queue.size).to eq(2)
      test_table.insert(:big_num => nums[2], :created_at => Time.now.utc)
      test_table.insert(:big_num => nums[3], :created_at => Time.now.utc)
      test_table.insert(:big_num => nums[4], :created_at => Time.now.utc)
      queue = []
      plugin.stop
      plugin.register
      plugin.run(queue)
      expect(queue.size).to eq(3)
    end
  end

  context "when iteratively running plugin#run with timestamp tracking column with column value" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"
      }
    end

    let(:settings) do
      { "statement" => "SELECT num, created_at, custom_time FROM test_table WHERE custom_time > :sql_last_value",
        "use_column_value" => true,
        "tracking_column" => "custom_time",
        "tracking_column_type" => "timestamp",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:nums) { [10, 20, 30, 40, 50] }
    let(:times) {["2015-05-06 13:14:15","2015-05-07 13:14:15","2015-05-08 13:14:15","2015-05-09 13:14:15","2015-05-10 13:14:15"]}

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should successfully update sql_last_value" do
      test_table = db[:test_table]

      plugin.run(queue)
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(Time.parse("1970-01-01 00:00:00.000000000 +0000"))
      test_table.insert(:num => nums[0], :created_at => Time.now.utc, :custom_time => times[0])
      test_table.insert(:num => nums[1], :created_at => Time.now.utc, :custom_time => times[1])
      plugin.run(queue)
      expect(plugin.instance_variable_get("@value_tracker").value.class).to eq(Time.parse(times[0]).class)
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(Time.parse(times[1]))
      test_table.insert(:num => nums[2], :created_at => Time.now.utc, :custom_time => times[2])
      test_table.insert(:num => nums[3], :created_at => Time.now.utc, :custom_time => times[3])
      test_table.insert(:num => nums[4], :created_at => Time.now.utc, :custom_time => times[4])
      plugin.run(queue)
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(Time.parse(times[4]))
    end
  end

  context "when iteratively running plugin#run with tracking_column and stored metadata" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"
      }
    end

    let(:settings) do
      { "statement" => "SELECT num, created_at FROM test_table WHERE num > :sql_last_value",
        "use_column_value" => true,
        "tracking_column" => "num",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:nums) { [10, 20, 30, 40, 50] }
    let(:last_run_value) { 20 }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should successfully update sql_last_value and only add appropriate events" do
      test_table = db[:test_table]

      plugin.run(queue)
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(20)
      expect(queue.length).to eq(0) # Shouldn't grab anything here.
      test_table.insert(:num => nums[0], :created_at => Time.now.utc)
      test_table.insert(:num => nums[1], :created_at => Time.now.utc)
      plugin.run(queue)
      expect(queue.length).to eq(0) # Shouldn't grab anything here either.
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(20)
      test_table.insert(:num => nums[2], :created_at => Time.now.utc)
      test_table.insert(:num => nums[3], :created_at => Time.now.utc)
      test_table.insert(:num => nums[4], :created_at => Time.now.utc)
      plugin.run(queue)
      expect(queue.length).to eq(3) # Only values greater than 20 should be grabbed.
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(50)
    end
  end

  context "when iteratively running plugin#run with BAD tracking_column and stored metadata" do
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true"
      }
    end

    let(:settings) do
      { "statement" => "SELECT num, created_at FROM test_table WHERE num > :sql_last_value",
        "use_column_value" => true,
        "tracking_column" => "not_num",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:nums) { [10, 20, 30, 40, 50] }
    let(:last_run_value) { 20 }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should send a warning and not update sql_last_value" do
      test_table = db[:test_table]

      plugin.run(queue)
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(20)
      expect(queue.length).to eq(0) # Shouldn't grab anything here.
      test_table.insert(:num => nums[0], :created_at => Time.now.utc)
      test_table.insert(:num => nums[1], :created_at => Time.now.utc)
      plugin.run(queue)
      expect(queue.length).to eq(0) # Shouldn't grab anything here either.
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(20)
      test_table.insert(:num => nums[2], :created_at => Time.now.utc)
      test_table.insert(:num => nums[3], :created_at => Time.now.utc)
      test_table.insert(:num => nums[4], :created_at => Time.now.utc)
      plugin.run(queue)
      expect(queue.length).to eq(3) # Only values greater than 20 should be grabbed.
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(20)
      expect(plugin.instance_variable_get("@tracking_column_warning_sent")).to eq(true)
    end
  end

  context "when previous runs are to be respected upon successful query execution (by time)" do

    let(:settings) do
      { "statement" => "SELECT 1 as num_param FROM SYSIBM.SYSDUMMY1",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:last_run_time) { Time.now.utc }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_time))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should respect last run metadata" do
      plugin.run(queue)

      expect(plugin.instance_variable_get("@value_tracker").value).to be > last_run_time
    end
  end

  context "when previous runs are to be respected upon successful query execution (by time string)" do

    let(:settings) do
      { "statement" => "SELECT custom_time FROM test_table WHERE custom_time > :sql_last_value",
        "use_column_value" => true,
        "tracking_column" => "custom_time",
        "tracking_column_type" => "timestamp",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:last_run_time) { '2010-03-19T14:48:40.483Z' }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_time))
      test_table = db[:test_table]
      test_table.insert(:num => 0, :custom_time => Time.now.utc)
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should respect last run metadata" do
      plugin.run(queue)
      expect(plugin.instance_variable_get("@value_tracker").value).to be > DateTime.parse(last_run_time).to_time
    end
  end

  context "when previous runs are to be respected upon successful query execution (by date/time string)" do

    let(:settings) do
      { "statement" => "SELECT custom_time FROM test_table WHERE custom_time > :sql_last_value",
        "use_column_value" => true,
        "tracking_column" => "custom_time",
        "tracking_column_type" => "timestamp",
        "jdbc_default_timezone" => "UTC", #this triggers the last_run_time to be treated as date/time
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:last_run_time) { '2010-03-19T14:48:40.483Z' }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_time))
      test_table = db[:test_table]
      test_table.insert(:num => 0, :custom_time => Time.now.utc)
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should respect last run metadata" do
      plugin.run(queue)
      expect(plugin.instance_variable_get("@value_tracker").value).to be > DateTime.parse(last_run_time)
    end
  end

  context "when previous runs are to be respected upon successful query execution (by column)" do

    let(:settings) do
      { "statement" => "SELECT 1 as num_param FROM SYSIBM.SYSDUMMY1",
        "use_column_value" => true,
        "tracking_column" => "num_param",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:last_run_value) { 1 }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "metadata should equal last_run_value" do
      plugin.run(queue)

      expect(plugin.instance_variable_get("@value_tracker").value).to eq(last_run_value)
    end
  end

  context "when previous runs are to be respected upon query failure (by time)" do
    let(:settings) do
      { "statement" => "SELECT col from non_existent_table",
        "last_run_metadata_path" => Stud::Temporary.pathname }
    end

    let(:last_run_time) { Time.now.utc }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_time))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should not respect last run metadata" do
      plugin.run(queue)

      expect(plugin.instance_variable_get("@value_tracker").value).to eq(last_run_time)
    end
  end

  context "when previous runs are to be respected upon query failure (by column)" do
    let(:settings) do
      { "statement" => "SELECT col from non_existent_table",
        "use_column_value" => true,
        "tracking_column" => "num_param",
        "last_run_metadata_path" => Stud::Temporary.pathname
      }
    end

    let(:last_run_value) { 1 }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "metadata should still reflect last value" do
      plugin.run(queue)

      expect(plugin.instance_variable_get("@value_tracker").value).to eq(last_run_value)
    end
  end

  context "when doing a clean run (by time)" do

    let(:settings) do
      {
        "statement" => "SELECT * FROM test_table",
        "last_run_metadata_path" => Stud::Temporary.pathname,
        "clean_run" => true
      }
    end

    let(:last_run_time) { Time.at(1).utc }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_time))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should ignore last run metadata if :clean_run set to true" do
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(Time.at(0).utc)
    end
  end

  context "when doing a clean run (by value)" do

    let(:settings) do
      {
        "statement" => "SELECT * FROM test_table",
        "last_run_metadata_path" => Stud::Temporary.pathname,
        "use_column_value" => true,
        "tracking_column" => "num_param",
        "clean_run" => true
      }
    end

    let(:last_run_value) { 1000 }

    before do
      File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should ignore last run metadata if :clean_run set to true" do
      expect(plugin.instance_variable_get("@value_tracker").value).to eq(0)
    end
  end


  context "when state is not to be persisted" do
    let(:settings) do
      {
        "statement" => "SELECT * FROM test_table",
        "last_run_metadata_path" => Stud::Temporary.pathname,
        "record_last_run" => false
      }
    end

    before do
      plugin.register
    end

    after do
      plugin.stop
    end

    it "should not save state if :record_last_run is false" do
      expect(File).not_to exist(settings["last_run_metadata_path"])
    end
  end

  context "when state is persisted" do
    context "to file" do
      let(:settings) do
        {
          "statement" => "SELECT * FROM test_table",
          "record_last_run" => true
        }
      end

      before do
        plugin.register
      end

      after do
        plugin.stop
      end

      context "with default last_run_metadata_path" do
        it "should save state in data.data subpath" do
          path = LogStash::SETTINGS.get_value("path.data")
          expect(plugin.last_run_metadata_file_path).to start_with(path)
        end
      end

      context "with customized last_run_metadata_path" do
        let(:settings) { super().merge({ "last_run_metadata_path" => Stud::Temporary.pathname })}

        it "should save state in data.data subpath" do
          expect(plugin.last_run_metadata_file_path).to start_with(settings["last_run_metadata_path"])
        end
      end
    end

    context "with customized last_run_metadata_path point to directory" do
      let(:settings) do
        path = Stud::Temporary.pathname
        Pathname.new(path).tap {|path| path.mkpath}
        super().merge({ "last_run_metadata_path" => path})
      end

      it "raise configuration error" do
        expect { plugin.register }.to raise_error(LogStash::ConfigurationError)
      end
    end
  end

  context "update the previous default last_run_metadata_path" do
    let(:settings) do
      {
        "statement" => "SELECT * FROM test_table",
        "record_last_run" => true
      }
    end

    let(:fake_home) do
       path = Stud::Temporary.pathname
       Pathname.new(path).tap {|path| path.mkpath}
       path
    end

    context "when a file exists" do
      before do
        # in a faked HOME folder save a valid previous last_run metadata file
        allow(ENV).to receive(:[]).with(anything).and_call_original
        allow(ENV).to receive(:[]).with('HOME').and_return(fake_home)

        File.open("#{fake_home}/.logstash_jdbc_last_run", 'w') do |file|
          file.write("--- !ruby/object:DateTime '2022-03-08 08:10:00.486889000 Z'")
        end
      end
      let(:old_path) { "#{fake_home}/.logstash_jdbc_last_run" }
      let(:path_data) { LogStash::SETTINGS.get_value("path.data") }
      let(:new_path) { "#{path_data}/plugins/inputs/jdbc/logstash_jdbc_last_run" }

      it "should be moved" do
        plugin.register
        expect(::File).to_not exist(old_path)
        expect(::File).to exist(new_path)
      end
      context "if the delete fails" do
        before(:each) do
          allow(File).to receive(:delete).and_raise ArgumentError
        end
        it "should be still be moved" do
          plugin.register
          expect(::File).to exist(old_path) # old still exists
          expect(::File).to exist(new_path)
        end
      end
    end
  end

  context "when setting fetch size" do

    let(:settings) do
      {
        "statement" => "SELECT * from test_table",
        "jdbc_fetch_size" => 1
      }
    end

    let(:num_rows) { 10 }

    before do
      num_rows.times do
        db[:test_table].insert(:num => 1, :custom_time => Time.now.utc, :created_at => Time.now.utc)
      end

      plugin.register
    end

    after do
      plugin.stop
    end

    it "should fetch all rows" do
      plugin.run(queue)
      expect(queue.size).to eq(num_rows)
    end
  end

  context "when driver is not found" do
    let(:settings) { { "statement" => "SELECT * FROM test_table" } }

    before do
      mixin_settings['jdbc_driver_class'] = "org.not.ExistsDriver"
    end

    it "should fail" do
      expect do
        plugin.register
        plugin.run(queue) # load when first run
      end.to raise_error(LogStash::PluginLoadingError)
    end
  end

  context "when timing out on connection" do
    let(:settings) do
      {
        "statement" => "SELECT * FROM test_table",
        "jdbc_pool_timeout" => 0,
        "jdbc_connection_string" => 'mock://localhost:1527/db',
        "sequel_opts" => {
          "max_connections" => 1
        }
      }
    end

    it "should raise PoolTimeout error" do
      plugin.register
      plugin.run(queue)
      db = plugin.instance_variable_get(:@database)
      expect(db.pool.instance_variable_get(:@timeout)).to eq(0)
      expect(db.pool.instance_variable_get(:@max_size)).to eq(1)

      q, q1 = Queue.new, Queue.new
      t = Thread.new{db.pool.hold{|c| q1.push nil; q.pop}}
      q1.pop
      expect{db.pool.hold {|c|}}.to raise_error(Sequel::PoolTimeout)
      q.push nil
      t.join
    end

    it "should log error message" do
      allow(Sequel).to receive(:connect).and_raise(Sequel::PoolTimeout)
      expect(plugin.logger).to receive(:error).with("Failed to connect to database. 0 second timeout exceeded. Tried 1 times.")
      expect do
        plugin.register
        plugin.run(queue)
      end.to raise_error(Sequel::PoolTimeout)
    end
  end

  context "when using logging" do

    let(:settings) do
      {
        "statement" => "SELECT * from test_table", "sql_log_level" => "debug"
      }
    end

    let(:num_rows) { 5 }

    before do
      allow(plugin.logger).to receive(:debug?)
      num_rows.times do
        db[:test_table].insert(:num => 1)
      end

      plugin.register
    end

    after do
      plugin.stop
    end

    it "should report the statements to logging" do
      expect(plugin.logger).to receive(:debug).once
      plugin.run(queue)
    end
  end

  describe "config option lowercase_column_names behaviour" do
    let(:settings) { { "statement" => "SELECT * FROM ttt" } }
    let(:events) { [] }

    before do
      db.create_table(:ttt) do
        Integer(:num)
        String(:somestring)
      end
      db[:ttt].insert(:num => 42, :somestring => "This is a string")
      plugin.register
    end

    after do
      plugin.stop
      db.drop_table(:ttt)
    end

    context "when lowercase_column_names is on (default)" do
      it "the field names are lower case" do
        plugin.run(events)
        expect(events.first.to_hash.keys.sort).to eq(
          ["@timestamp", "@version","num", "somestring"])
      end
    end

    context "when lowercase_column_names is off" do
      let(:settings) { { "statement" => "SELECT * FROM ttt", "lowercase_column_names" => false } }
      it "the field names are UPPER case (natural for Derby DB)" do
        plugin.run(events)
        expect(events.first.to_hash.keys.sort).to eq(
          ["@timestamp", "@version","NUM", "SOMESTRING"])
      end
    end
  end

  context "when specifying connection_retry_attempts" do
    let(:settings) { {"statement" => "SELECT 1 as col1 FROM test_table"} }

    it "should try to connect connection_retry_attempts times" do
      mixin_settings['connection_retry_attempts'] = 2
      mixin_settings['jdbc_pool_timeout'] = 0
      allow(Sequel).to receive(:connect).and_raise(Sequel::PoolTimeout)
      expect(plugin.logger).to receive(:error).with("Failed to connect to database. 0 second timeout exceeded. Trying again.")
      expect(plugin.logger).to receive(:error).with("Failed to connect to database. 0 second timeout exceeded. Tried 2 times.")
      expect do
        plugin.register
        plugin.run(queue)
      end.to raise_error(Sequel::PoolTimeout)
    end

    it "should not fail when passed a non-positive value" do
      mixin_settings['connection_retry_attempts'] = -2
      expect { plugin.register }.to_not raise_error
      plugin.stop
    end

    it "does retry when query execution fails" do
      mixin_settings['statement_retry_attempts'] = 2
      mixin_settings['statement_retry_attempts_wait_time'] = 0.5
      queue = Queue.new
      plugin.register

      handler = plugin.instance_variable_get(:@statement_handler)
      allow(handler).to receive(:perform_query).with(instance_of(Sequel::JDBC::Database), instance_of(Time)).and_raise(Sequel::PoolTimeout)
      expect(plugin.logger).to receive(:error).with("Unable to execute statement. Trying again.")
      expect(plugin.logger).to receive(:error).with("Unable to execute statement. Tried 2 times.")

      plugin.run(queue)
      plugin.stop
    end

    it "does not retry when query execution succeeds" do
      mixin_settings['connection_retry_attempts'] = 2
      queue = Queue.new
      plugin.register

      handler = plugin.instance_variable_get(:@statement_handler)
      allow(handler).to receive(:perform_query).with(instance_of(Sequel::JDBC::Database), instance_of(Time)).and_call_original
      expect(plugin.logger).not_to receive(:error)

      plugin.run(queue)
      plugin.stop
    end
  end

  context "when encoding of some columns need to be changed" do

    let(:settings) {{ "statement" => "SELECT * from test_table" }}
    let(:events)   { [] }
    let(:row) do
      {
        "column0" => "foo",
        "column1" => "bar".force_encoding(Encoding::ISO_8859_1),
        "column2" => 3
      }
    end

    before(:each) do
      dataset = double("Dataset")
      allow(dataset).to receive(:each).and_yield(row)
      allow(plugin).to receive(:jdbc_connect).and_wrap_original do |m, *args|
        _db = m.call(*args)
        allow(_db).to receive(:[]).and_return(dataset)
        _db
      end
      # allow_any_instance_of(Sequel::JDBC::Derby::Dataset).to receive(:each).and_yield(row)
      plugin.register
    end

    after(:each) do
      plugin.stop
    end

    it "should not convert any column by default" do
      encoded_row = {
        "column0" => "foo",
        "column1" => "bar".force_encoding(Encoding::ISO_8859_1),
        "column2" => 3
      }
      event = LogStash::Event.new(row)
      expect(LogStash::Event).to receive(:new) do |row|
        row.each do |k, v|
          next unless v.is_a?(String)
          expect(row[k].encoding).to eq(encoded_row[k].encoding)
        end

        event
      end
      plugin.run(events)
    end

    context "when all string columns should be encoded" do

      let(:settings) do
        {
          "statement" => "SELECT * from test_table",
          "charset" => "ISO-8859-1"
        }
      end

      let(:row) do
        {
          "column0" => "foo".force_encoding(Encoding::ISO_8859_1),
          "column1" => "bar".force_encoding(Encoding::ISO_8859_1),
          "column2" => 3
        }
      end

      it "should transform all column string to UTF-8, default encoding" do
        encoded_row = {
          "column0" => "foo",
          "column1" => "bar",
          "column2" => 3
        }
        event = LogStash::Event.new(row)
        expect(LogStash::Event).to receive(:new) do |row|
          row.each do |k, v|
            next unless v.is_a?(String)
            expect(row[k].encoding).to eq(encoded_row[k].encoding)
          end

          event
        end
        plugin.run(events)
      end
    end

    context "when only an specific column should be converted" do

      let(:settings) do
        {
          "statement" => "SELECT * from test_table",
          "columns_charset" => { "column1" => "ISO-8859-1" }
        }
      end

      let(:row) do
        {
          "column0" => "foo",
          "column1" => "bar".force_encoding(Encoding::ISO_8859_1),
          "column2" => 3,
          "column3" => "berlin".force_encoding(Encoding::ASCII_8BIT)
        }
      end

      it "should only convert the selected column" do
        encoded_row = {
          "column0" => "foo",
          "column1" => "bar",
          "column2" => 3,
          "column3" => "berlin".force_encoding(Encoding::ASCII_8BIT)
        }
        event = LogStash::Event.new(row)
        expect(LogStash::Event).to receive(:new) do |row|
          row.each do |k, v|
            next unless v.is_a?(String)
            expect(row[k].encoding).to eq(encoded_row[k].encoding)
          end

          event
        end
        plugin.run(events)
      end
    end
  end

  context "when fetching Various Typed data" do

    let(:settings) do
      {
      "statement" => "SELECT * from types_table"
      }
    end

    before do
      db << "INSERT INTO types_table (num, string, started_at, custom_time, ranking, uuid) VALUES (1, 'A test', '1999-12-31', '1999-12-31 23:59:59', 95.67, '018f15f3-6cfd-7a1b-b70f-d97ed8a73128')"

      plugin.register
    end

    after do
      plugin.stop
    end

    it "should convert all columns to valid Event acceptable data types" do
      plugin.run(queue)
      event = queue.pop
      expect(event.get("num")).to eq(1)
      expect(event.get("string")).to eq("A test")
      expect(event.get("started_at")).to be_a_logstash_timestamp_equivalent_to("1999-12-31T00:00:00.000Z")
      expect(event.get("custom_time")).to be_a_logstash_timestamp_equivalent_to("1999-12-31T23:59:59.000Z")
      expect(event.get("ranking").to_f).to eq(95.67)
      expect(event.get("uuid")).to eq("018f15f3-6cfd-7a1b-b70f-d97ed8a73128")
    end
  end

  context "when retrieving records with ambiguous timestamps" do

    let(:settings) do
      {
        "statement" => "SELECT * from types_table",
        "jdbc_default_timezone" => jdbc_default_timezone
      }
    end

    before(:each) do
      db << "INSERT INTO types_table (num, string, started_at, custom_time, ranking, uuid) VALUES (1, 'A test', '1999-12-31', '2021-11-07 01:23:45', 95.67, '018f15f3-6cfd-7a1b-b70f-d97ed8a73128')"
      plugin.register
    end

    context "when initialized with a preference for DST being enabled" do
      let(:jdbc_default_timezone) { 'America/Chicago[dst_enabled_on_overlap:true]' }

      it 'treats the timestamp column as if DST was enabled' do
        plugin.run(queue)
        event = queue.pop
        expect(event.get("custom_time")).to be_a_logstash_timestamp_equivalent_to("2021-11-07T06:23:45Z")
      end
    end
    context "when initialized with a preference for DST being disabled" do
      let(:jdbc_default_timezone) { 'America/Chicago[dst_enabled_on_overlap:false]' }

      it 'treats the timestamp column as if DST was disabled' do
        plugin.run(queue)
        event = queue.pop
        expect(event.get("custom_time")).to be_a_logstash_timestamp_equivalent_to("2021-11-07T07:23:45Z")
      end
    end
    context "when initialized without a preference for DST being enabled or disabled" do
      before(:each) { allow(plugin.logger).to receive(:warn) }
      let(:jdbc_default_timezone) { 'America/Chicago' }

      it 'the error results in helpful log warning' do
        plugin.run(queue)
        expect(plugin.logger).to have_received(:warn).with(a_string_including("Exception when executing JDBC query"), a_hash_including(:message => a_string_including("2021-11-07 01:23:45 is an ambiguous local time")))
      end
    end
  end

  def load_derby_version
    version = {}
    derby_version =  File.join(Dir.pwd, 'derby_version.txt')
    File.readlines(derby_version, chomp: true).each do |line|
      key = line.split('=')[0]
      value = line.split('=')[1]
      version[key] = value
    end
    version
  end

  context "when an unreadable jdbc_driver_path entry is present" do
    let(:driver_jar_path) do
      derby_version = load_derby_version()['DERBY_VERSION']
      jar_file = $CLASSPATH.find { |name| name.index("derby-#{derby_version}.jar") }
      raise "derby jar not found on class-path" unless jar_file
      jar_file.sub('file:', '')
    end

    let(:invalid_driver_jar_path) do
      path = File.join(Dir.mktmpdir, File.basename(driver_jar_path))
      FileUtils.cp driver_jar_path, path
      FileUtils.chmod "u=x,go=", path
      path
    end

    let(:settings) do
      { "statement" => "SELECT * from types_table", "jdbc_driver_library" => invalid_driver_jar_path }
    end

    after do
      plugin.stop
    end

    it "raise a loading error" do
      expect(File.exists?(invalid_driver_jar_path)).to be true
      expect(FileTest.readable?(invalid_driver_jar_path)).to be false

      expect { plugin.register }.
          to raise_error(LogStash::PluginLoadingError, /unable to load .*? from :jdbc_driver_library, file not readable/)
    end
  end

  context "when using prepared statements" do
    let(:last_run_value) { 250 }
    let(:expected_queue_size) { 100 }
    let(:num_rows) { 1000 }

    context "check validation" do
      context "with an empty name setting" do
        let(:settings) do
          {
            "statement" => "SELECT * FROM test_table ORDER BY num FETCH NEXT ? ROWS ONLY",
            "prepared_statement_bind_values" => [100],
            "use_prepared_statements" => true,
          }
        end

        it "should fail to register" do
          expect{ plugin.register }.to raise_error(LogStash::ConfigurationError)
        end
      end

      context "with an mismatched placeholder vs bind values" do
        let(:settings) do
          {
            "statement" => "SELECT * FROM test_table ORDER BY num FETCH NEXT ? ROWS ONLY",
            "prepared_statement_bind_values" => [],
            "use_prepared_statements" => true,
          }
        end

        it "should fail to register" do
          expect{ plugin.register }.to raise_error(LogStash::ConfigurationError)
        end
      end

      context "with jdbc paging enabled" do
        let(:settings) do
          {
            "statement" => "SELECT * FROM test_table ORDER BY num FETCH NEXT 100 ROWS ONLY",
            "prepared_statement_bind_values" => [],
            "prepared_statement_name" => "pstmt_test_without",
            "use_prepared_statements" => true,
            "jdbc_paging_enabled" => true,
            "jdbc_page_size" => 20,
            "jdbc_fetch_size" => 10
          }
        end

        it "should fail to register" do
          expect{ plugin.register }.to raise_error(LogStash::ConfigurationError)
        end
      end

    end

    context "and no validation failures" do
      before do
        ::File.write(settings["last_run_metadata_path"], YAML.dump(last_run_value))
        num_rows.times do |n|
          db[:test_table].insert(:num => n.succ, :string => SecureRandom.hex(8), :custom_time => Time.now.utc, :created_at => Time.now.utc)
        end
      end

      after do
        plugin.stop
      end

      context "with jdbc paging enabled" do
        let(:settings) do
          {
            "statement" => "SELECT * FROM test_table ORDER BY num FETCH NEXT 100 ROWS ONLY",
            "prepared_statement_bind_values" => [],
            "prepared_statement_name" => "pstmt_test_without",
            "use_prepared_statements" => true,
            "tracking_column_type" => "numeric",
            "tracking_column" => "num",
            "use_column_value" => true,
            "last_run_metadata_path" => Stud::Temporary.pathname,
            "jdbc_paging_enabled" => true,
            "jdbc_page_size" => 20,
            "jdbc_fetch_size" => 10
          }
        end

        it "should fail to register" do
          expect{ plugin.register }.to raise_error(LogStash::ConfigurationError)
        end
      end

      context "without placeholder and bind parameters" do
        let(:settings) do
          {
            "statement" => "SELECT * FROM test_table ORDER BY num FETCH NEXT 100 ROWS ONLY",
            "prepared_statement_bind_values" => [],
            "prepared_statement_name" => "pstmt_test_without",
            "use_prepared_statements" => true,
            "tracking_column_type" => "numeric",
            "tracking_column" => "num",
            "use_column_value" => true,
            "last_run_metadata_path" => Stud::Temporary.pathname
          }
        end

        it "should fetch some rows" do
          plugin.register
          plugin.run(queue)

          expect(queue.size).to eq(expected_queue_size)
          expect(LogStash::PluginMixins::Jdbc::ValueTracking.load_yaml(File.read(settings["last_run_metadata_path"]))).to eq(expected_queue_size)
        end
      end


      context "with bind parameters" do
        let(:settings) do
          {
            # Sadly, postgres does but derby doesn't - It is not allowed for both operands of '+' to be ? parameters.: PREPARE pstmt_test: SELECT * FROM test_table WHERE (num > ?) AND (num <= ? + ?) ORDER BY num
            "statement" => "SELECT * FROM test_table WHERE (num > ?) ORDER BY num FETCH NEXT ? ROWS ONLY",
            "prepared_statement_bind_values" => [":sql_last_value", expected_queue_size],
            "prepared_statement_name" => "pstmt_test_with",
            "use_prepared_statements" => true,
            "tracking_column_type" => "numeric",
            "tracking_column" => "num",
            "use_column_value" => true,
            "last_run_metadata_path" => Stud::Temporary.pathname
          }
        end

        it "should fetch some rows" do
          plugin.register
          plugin.run(queue)

          expect(queue.size).to eq(expected_queue_size)
          expect(LogStash::PluginMixins::Jdbc::ValueTracking.load_yaml(File.read(settings["last_run_metadata_path"]))).to eq(last_run_value + expected_queue_size)
        end
      end
    end
  end

  describe "jdbc_driver_class" do
    context "when not prefixed with Java::" do
      let(:jdbc_driver_class) { "org.apache.derby.jdbc.EmbeddedDriver" }
      it "loads the class" do
        expect { plugin.send(:load_driver) }.not_to raise_error
      end
    end
    context "when prefixed with Java::" do
      let(:jdbc_driver_class) { "Java::org.apache.derby.jdbc.EmbeddedDriver" }
      it "loads the class" do
        expect { plugin.send(:load_driver) }.not_to raise_error
      end
    end
    context "when prefixed with Java." do
      let(:jdbc_driver_class) { "Java.org::apache::derby::jdbc.EmbeddedDriver" }
      it "loads the class" do
        expect { plugin.send(:load_driver) }.not_to raise_error
      end

      it "can instantiate the returned driver class" do
        # for drivers where the path through DriverManager fails, Sequel assumes
        # having a proxied Java class instance (instead of a java.lang.Class) and
        # does a driver.new.connect https://git.io/JDV6M
        driver = plugin.send(:load_driver)
        expect { driver.new }.not_to raise_error
      end
    end
    context "when class name invalid" do
      let(:jdbc_driver_class) { "org.apache.NonExistentDriver" }
      it "raises a loading error" do
        expect { plugin.send(:load_driver) }.to raise_error LogStash::PluginLoadingError,
                                                            /ClassNotFoundException: org.apache.NonExistentDriver/
      end
    end
  end
end
