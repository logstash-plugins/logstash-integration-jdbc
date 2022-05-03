# encoding: utf-8

module LogStash module PluginMixins module Jdbc
  class StatementHandler
    def self.build_statement_handler(plugin)
      if plugin.use_prepared_statements
        klass = PreparedStatementHandler
      else
        if plugin.jdbc_paging_enabled
          if plugin.jdbc_paging_mode == "explicit"
            klass = ExplicitPagingModeStatementHandler
          else
            klass = PagedNormalStatementHandler
          end
        else
          klass = NormalStatementHandler
        end
      end
      klass.new(plugin)
    end

    attr_reader :statement, :parameters

    def initialize(plugin)
      @statement = plugin.statement
    end

    def build_query(db, sql_last_value)
      fail NotImplementedError # override in subclass
    end

  end

  class NormalStatementHandler < StatementHandler

    attr_reader :parameters

    def initialize(plugin)
      super(plugin)
      @parameter_keys = ["sql_last_value"] + plugin.parameters.keys
      @parameters = plugin.parameters.inject({}) do |hash,(k,v)|
        case v
        when LogStash::Timestamp
          hash[k.to_sym] = v.time
        else
          hash[k.to_sym] = v
        end
        hash
      end
    end

    # Performs the query, yielding once per row of data
    # @param db [Sequel::Database]
    # @param sql_last_value [Integer|DateTime|Time]
    # @yieldparam row [Hash{Symbol=>Object}]
    def perform_query(db, sql_last_value)
      query = build_query(db, sql_last_value)
      query.each do |row|
        yield row
      end
    end

    private

    def build_query(db, sql_last_value)
      parameters[:sql_last_value] = sql_last_value
      db[statement, parameters]
    end

  end

  class PagedNormalStatementHandler < NormalStatementHandler

    def initialize(plugin)
      super(plugin)
      @jdbc_page_size = plugin.jdbc_page_size
      @logger = plugin.logger
    end

    # Performs the query, respecting our pagination settings, yielding once per row of data
    # @param db [Sequel::Database]
    # @param sql_last_value [Integer|DateTime|Time]
    # @yieldparam row [Hash{Symbol=>Object}]
    def perform_query(db, sql_last_value)
      query = build_query(db, sql_last_value)
      query.each_page(@jdbc_page_size) do |paged_dataset|
        log_dataset_page(paged_dataset) if @logger.debug?
        paged_dataset.each do |row|
          yield row
        end
      end
    end

    private

    # @param paged_dataset [Sequel::Dataset::Pagination] like object
    def log_dataset_page(paged_dataset)
      @logger.debug "fetching paged dataset", current_page: paged_dataset.current_page,
                                              record_count: paged_dataset.current_page_record_count,
                                              total_record_count: paged_dataset.pagination_record_count
    end

  end

  class ExplicitPagingModeStatementHandler < PagedNormalStatementHandler
    # Performs the query, respecting our pagination settings, yielding once per row of data
    # @param db [Sequel::Database]
    # @param sql_last_value [Integer|DateTime|Time]
    # @yieldparam row [Hash{Symbol=>Object}]
    def perform_query(db, sql_last_value)
      query = build_query(db, sql_last_value)
      offset = 0
      page_size = @jdbc_page_size
      loop do
        rows_in_page = 0
        query.with_sql(query.sql, offset: offset, size: page_size).each do |row|
          yield row
          rows_in_page += 1
        end
        break unless rows_in_page == page_size
        offset += page_size
      end
    end
  end

  class PreparedStatementHandler < StatementHandler
    attr_reader :name, :bind_values_array, :statement_prepared, :prepared, :parameters

    def initialize(plugin)
      super(plugin)
      @name = plugin.prepared_statement_name.to_sym
      @bind_values_array = plugin.prepared_statement_bind_values
      @parameters = plugin.parameters
      @statement_prepared = Concurrent::AtomicBoolean.new(false)
    end

    # Performs the query, ignoring our pagination settings, yielding once per row of data
    # @param db [Sequel::Database]
    # @param sql_last_value [Integet|DateTime|Time]
    # @yieldparam row [Hash{Symbol=>Object}]
    def perform_query(db, sql_last_value)
      query = build_query(db, sql_last_value)
      query.each do |row|
        yield row
      end
    end

    private

    def build_query(db, sql_last_value)
      @parameters = create_bind_values_hash
      if statement_prepared.false?
        prepended = parameters.keys.map{|v| v.to_s.prepend("$").to_sym}
        @prepared = db[statement, *prepended].prepare(:select, name)
        statement_prepared.make_true
      end
      # under the scheduler the Sequel database instance is recreated each time
      # so the previous prepared statements are lost, add back
      if db.prepared_statement(name).nil?
        db.set_prepared_statement(name, prepared)
      end
      bind_value_sql_last_value(sql_last_value)
      begin
        db.call(name, parameters)
      rescue => e
        # clear the statement prepared flag - the statement may be closed by this
        # time.
        statement_prepared.make_false
        raise e
      end
    end

    def create_bind_values_hash
      hash = {}
      bind_values_array.each_with_index {|v,i| hash[:"p#{i}"] = v}
      hash
    end

    def bind_value_sql_last_value(sql_last_value)
      parameters.keys.each do |key|
        value = parameters[key]
        if value == ":sql_last_value"
          parameters[key] = sql_last_value
        end
      end
    end
  end
end end end
