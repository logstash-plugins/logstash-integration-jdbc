require_relative "basic_database"

module LogStash module Filters module Jdbc
  class ReadWriteDatabase < BasicDatabase
    def repopulate_all(loaders)
      case loaders.size
        when 1
          fill_and_switch(loaders.first)
        when 2
          fill_and_switch(loaders.first)
          fill_and_switch(loaders.last)
        else
          loaders.each do |loader|
            fill_and_switch(loader)
          end
      end
    end

    alias populate_all repopulate_all

    def fetch(statement, parameters)
      @rwlock.readLock().lock()
      # any exceptions should bubble up because we need to set failure tags etc.
      @db[statement, parameters].all
    ensure
      @rwlock.readLock().unlock()
    end

    def build_db_object(db_object)
      begin
        @rwlock.writeLock().lock()
        db_object.build(@db)
      rescue *CONNECTION_ERRORS => err
        # we do not raise an error when there is a connection error, we hope that the connection works next time
        logger.error("Connection error when initialising lookup db", :db_object => db_object.inspect, :exception => err.message, :backtrace => err.backtrace.take(8))
      rescue ::Sequel::Error => err
        msg = "Exception when initialising lookup db for db object: #{db_object}"
        logger.error(msg, :exception => err.message, :backtrace => err.backtrace.take(8))
        raise wrap_error(LoaderJdbcException, err, msg)
      ensure
        @rwlock.writeLock().unlock()
      end
    end

    def post_create(connection_string, driver_class, driver_library, user, password)
      mutated_connection_string = connection_string.sub("____", unique_db_name)
      verify_connection(mutated_connection_string, driver_class, driver_library, user, password)
      connect("Connection error when connecting to lookup db")
    end

    private

    def fill_and_switch(loader)
      begin
        records = loader.fetch
        return if records.size.zero?
        @rwlock.writeLock().lock()
        tmp = self.class.random_name
        @db.transaction do |conn|
          @db[loader.temp_table].multi_insert(records)
          @db.rename_table(loader.temp_table, tmp)
          @db.rename_table(loader.table, loader.temp_table)
          @db.rename_table(tmp, loader.table)
          @db[loader.temp_table].truncate
        end
      rescue *CONNECTION_ERRORS => err
        # we do not raise an error when there is a connection error, we hope that the connection works next time
        logger.error("Connection error when filling lookup db from loader query results", :exception => err.message, :backtrace => err.backtrace.take(8))
      rescue => err
        # In theory all exceptions in Sequel should be wrapped in Sequel::Error
        # There are cases where exceptions occur in unprotected ensure sections
        msg = "Exception when filling lookup db from loader query results, original exception: #{err.class}, original message: #{err.message}"
        logger.error(msg, :backtrace => err.backtrace.take(16))
        raise wrap_error(LoaderJdbcException, err, msg)
      ensure
        @rwlock.writeLock().unlock()
      end
    end

    def post_initialize()
      super
      # get a fair reentrant read write lock
      @rwlock = java.util.concurrent.locks.ReentrantReadWriteLock.new(true)
    end
  end
end end end
