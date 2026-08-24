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
    attr_reader :name

    def initialize(plugin)
      super(plugin)
      @name = plugin.prepared_statement_name.to_sym

      @positional_bind_mapping =  create_positional_bind_mapping(plugin.prepared_statement_bind_values).freeze
      @positional_bind_placeholders = @positional_bind_mapping.keys.map { |v| :"$#{v}" }.freeze
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
      # under the scheduler the Sequel database instance is recreated each time
      # so the previous prepared statements are lost, add back
      prepared = db.prepared_statement(name)
      prepared ||= db[statement, *positional_bind_placeholders].prepare(:select, name)

      prepared.call(positional_bind_mapping(sql_last_value))
    end

    def create_positional_bind_mapping(bind_values_array)
      hash = {}
      bind_values_array.each_with_index {|v,i| hash[:"p#{i}"] = v}
      hash
    end

    def positional_bind_mapping(sql_last_value)
      @positional_bind_mapping.transform_values do |value|
        value == ":sql_last_value" ? sql_last_value : value
      end
    end

    def positional_bind_placeholders
      @positional_bind_mapping.keys.map { |v| :"$#{v}" }.freeze
    end
  end
end end end
