require_relative 'spec_helper'
require 'tempfile'

describe LogStash::Inputs::Jdbc, :integration => true do
  # This is a necessary change test-wide to guarantee that no local timezone
  # is picked up.  It could be arbitrarily set to any timezone, but then the test
  # would have to compensate differently.  That's why UTC is chosen.
  before(:all) { ENV['TZ'] = "Etc/UTC" }
  after(:all) { ENV['TZ'] = ENV_TZ }

  # For Travis and CI based on docker, we source from ENV
  jdbc_connection_string = ENV.fetch("PG_CONNECTION_STRING",
                                     "jdbc:postgresql://postgresql:5432") + "/jdbc_input_db?user=postgres"

  let(:settings) do
    { "jdbc_driver_class" => "org.postgresql.Driver",
      "jdbc_connection_string" => jdbc_connection_string,
      "jdbc_driver_library" => ENV['POSTGRES_DRIVER_JAR'] || "/usr/share/logstash/postgresql.jar",
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
  end

  context "when supplying a non-existent library" do
    let(:settings) do
      super.merge(
          "jdbc_driver_library" => "/no/path/to/postgresql.jar"
      )
    end

    it "should not register correctly" do
      plugin.register
      q = Queue.new
      expect do
        plugin.run(q)
      end.to raise_error(::LogStash::PluginLoadingError)
    end
  end

  context "when connecting to a non-existent server" do
    let(:settings) do
      super.merge(
          "jdbc_connection_string" => "jdbc:postgresql://localhost:65000/somedb"
      )
    end

    it "should not register correctly" do
      plugin.register
      q = Queue.new
      expect do
        plugin.run(q)
      end.to raise_error(::Sequel::DatabaseConnectionError)
    end
  end

  context "using sql last value" do

    let(:last_run_metadata_file) do
      Tempfile.new('last_run_metadata')
    end

    before do
      plugin.register
    end

    after do
      last_run_metadata_file.close rescue nil
      File.unlink(last_run_metadata_file.path) rescue nil
      plugin.stop
    end

    context '(DATE)' do

      let(:settings) do
        super.merge('use_column_value' => true,
                    'tracking_column' => "created_at",
                    'tracking_column_type' => "timestamp",
                    'jdbc_default_timezone' => "America/New_York", # ENV["TZ"] is "Etc/UTC"
                    'schedule' => "*/2 * * * * *", # every 2 seconds
                    'last_run_metadata_path' => last_run_metadata_file.path,
                    'statement' => "SELECT * FROM employee WHERE created_at > :sql_last_value ORDER BY created_at")
      end

      it "should populate the event with database entries" do
        Thread.start { plugin.run(queue) }

        sleep(2.5)

        expect( queue.size ).to be >= 4
        event = queue.pop
        expect(event.get('first_name')).to eq("Mark")
        event = queue.pop
        expect(event.get('first_name')).to eq("David")
        event = queue.pop
        expect(event.get('first_name')).to eq("Ján")
        expect(event.get('created_at').to_s).to eql '2000-02-01T00:00:00.000Z'
        event = queue.pop
        expect(event.get('first_name')).to eq("Jožko")

        expect( last_run_value = read_last_run_metadata_yaml ).to be >= DateTime.new(2010)
        expect( last_run_value.zone ).to eql '+00:00'

        expect( queue.size ).to be 0

        begin
          delete_test_employee_data!(plugin.database)
          insert_test_employee_data!(plugin.database, now = Date.today, :created_at)

          sleep(2.0)

          # TODO will return "emp_no" => 4 ("Jožko") again
          # due SELECT * FROM employee WHERE created_at > '2009-12-31 19:00:00.000000-0500'
          # expect( queue.size ).to eql 3
          #
          # event = queue.pop
          # expect(event.get('first_name')).to eq("3")
          # event = queue.pop
          # expect(event.get('first_name')).to eq("2")
          # event = queue.pop
          # expect(event.get('first_name')).to eq("1")

          expect( read_last_run_metadata_yaml ).to be > last_run_value
          expect( read_last_run_metadata_yaml ).to be > now

        ensure
          delete_test_employee_data!(plugin.database)
        end
      end

    end

    context '(TIMESTAMP)' do

      let(:settings) do
        super.merge('use_column_value' => true,
                    'tracking_column' => "updated_at",
                    'tracking_column_type' => "timestamp",
                    'jdbc_default_timezone' => "Europe/Paris", # ENV["TZ"] is "Etc/UTC"
                    'schedule' => "*/1 * * * * *", # every second
                    'last_run_metadata_path' => last_run_metadata_file.path,
                    'statement' => "SELECT * FROM employee WHERE updated_at IS NOT NULL "+
                                   "AND updated_at > :sql_last_value ORDER BY updated_at")
      end

      it "should populate the event with database entries" do
        Thread.start { plugin.run(queue) }

        sleep(1.75)

        expect( queue.size ).to be 3
        event = queue.pop
        expect(event.get('first_name')).to eq("David")
        event = queue.pop
        expect(event.get('first_name')).to eq("Mark")
        event = queue.pop
        expect(event.get('first_name')).to eq("Ján")

        if ENV['TZ'].nil? && Time.new.utc_offset == 60 * 60
          # for local TZ != UTC this gets adjusted: 2000-01-31T23:00:00.000Z
          expect(event.get('created_at').to_s).to eql '2000-01-31T23:00:00.000Z'
          # expect(event.get('updated_at').to_s).to eql '2020-01-31T19:30:40.000Z' # DateTime -> 2020-01-31T20:30:40.000Z
        else # assume TZ = UTC
          expect(event.get('created_at').to_s).to eql '2000-02-01T00:00:00.000Z'
        end

        last_run_value = read_last_run_metadata_yaml
        puts "(1) last_run_value: #{last_run_value.inspect} - #{last_run_value.class}" # 2020-01-31 19:30:40 UTC - Time

        expect( last_run_value.to_datetime ).to be >= DateTime.new(2020, 01, 31, 19, 30, 40)
        expect( last_run_value.to_datetime.zone ).to eql '+00:00'

        begin
          delete_test_employee_data!(plugin.database)
          insert_test_employee_data!(plugin.database, now = Time.now, :updated_at)
          plugin.database.run "INSERT INTO employee VALUES (42, '42', 'user', CURRENT_DATE, '2020-01-31 20:32:40')"

          sleep(1.0)

          expect( queue.size ).to eql 4

          event = queue.pop
          expect(event.get('first_name')).to eq("42")
          event = queue.pop
          expect(event.get('first_name')).to eq("3")
          event = queue.pop
          expect(event.get('first_name')).to eq("2")
          event = queue.pop
          expect(event.get('first_name')).to eq("1")

          last_run_value2 = read_last_run_metadata_yaml
          puts "(2) last_run_value: #{last_run_value2.inspect} - #{last_run_value2.class}" # ??? 2020-01-31 20:30:40 +0000 - Time

          # e.g. #<DateTime: 2020-11-17T10:03:17+00:00 ...>
          expect( read_last_run_metadata_yaml ).to be > last_run_value
          expect( read_last_run_metadata_yaml.to_time ).to be > now

        ensure
          delete_test_employee_data!(plugin.database)
        end
      end

    end

    def insert_test_employee_data!(db, now, row)
      db[:employee].insert(:emp_no => 10, :first_name => '2', :last_name => 'user', row => now - 1)
      db[:employee].insert(:emp_no => 11, :first_name => '3', :last_name => 'user', row => now - 2)
      db[:employee].insert(:emp_no => 12, :first_name => '1', :last_name => 'user', row => now + 1)
    end

    def delete_test_employee_data!(db)
      db[:employee].where(:last_name => 'user').delete
    end

    def read_last_run_metadata_yaml
      # "--- !ruby/object:DateTime '2020-11-17 07:56:23.978705000 Z'\n"
      YAML.load(File.read(last_run_metadata_file.path))
    end

  end
end

