# encoding: utf-8
require_relative "lookup_result"
require "logstash/util/loggable"

module LogStash module Filters module Jdbc
  class Lookup
    include LogStash::Util::Loggable

    class Sprintfier
      def initialize(param)
        @param = param
      end

      def fetch(event, result)
        formatted = event.sprintf(@param)
        if formatted == @param # no field found so no transformation
          result.invalid_parameters_push(@param)
        end
        formatted
      end
    end

    class Getfier
      def initialize(param)
        @param = param
      end

      def fetch(event, result)
        value = event.get(@param)
        if value.nil? || value.is_a?(Hash) || value.is_a?(Array) # Array or Hash is not suitable
          result.invalid_parameters_push(@param)
        end
        value
      end
    end

    def self.find_validation_errors(array_of_options)
      if !array_of_options.is_a?(Array)
        return "The options must be an Array"
        end
      errors = []
      array_of_options.each_with_index do |options, i|
        instance = new(options, {}, "lookup-#{i.next}")
        unless instance.valid?
          errors << instance.formatted_errors
        end
      end
      return nil if errors.empty?
      errors.join("; ")
    end

    attr_reader :id, :target, :query, :parameters

    def initialize(options, globals, default_id)
      @id = options["id"] || default_id
      @target = options["target"]
      @id_used_as_target = @target.nil?
      if @id_used_as_target
        # target shouldn't be nil if ecs_compatibility is not :disabled
        if globals[:ecs_compatibility] != :disabled
          logger.info('ECS compatibility is enabled but no ``target`` option was specified, it is recommended'\
                            ' to set the option to avoid potential schema conflicts (if your data is ECS compliant or'\
                            ' non-conflicting feel free to ignore this message)')
        end
        @target = @id
      end
      @options = options
      @globals = globals
      @valid = false
      @option_errors = []
      @default_result = nil
      @prepared_statement = nil
      @symbol_parameters = nil
      parse_options
      @load_method_ref = method(:load_data_from_local)
    end

    def id_used_as_target?
      @id_used_as_target
    end

    def valid?
      @valid
    end

    def formatted_errors
      @option_errors.join(", ")
    end

    def enhance(local, event)
      result = retrieve_local_data(local, event, &@load_method_ref) # return a LookupResult
      if result.failed? || result.parameters_invalid?
        tag_failure(event)
      end

      if result.valid?
        if @use_default && result.empty?
          tag_default(event)
          process_event(event, @default_result)
        else
          process_event(event, result)
        end
        true
      else
        false
      end
    end

    def use_prepared_statement?
      @prepared_parameters && !@prepared_parameters.empty?
    end

    def prepare(local)
      hash = {}
      @prepared_parameters.each_with_index { |v, i| hash[:"$p#{i}"] = v }
      @prepared_param_placeholder_map = hash
      @prepared_statement = local.prepare(query, hash.keys)
      @load_method_ref = method(:load_data_from_prepared)
    end

    private

    def tag_failure(event)
      @tag_on_failure.each do |tag|
        event.tag(tag)
      end
    end

    def tag_default(event)
      @tag_on_default_use.each do |tag|
        event.tag(tag)
      end
    end

    def load_data_from_local(local, query, params, result)
      local.fetch(query, params).each do |row|
        stringified = row.inject({}){|hash,(k,v)| hash[k.to_s] = v; hash} #Stringify row keys
        result.push(stringified)
      end
    end

    def load_data_from_prepared(_local, _query, params, result)
      @prepared_statement.call(params).each do |row|
        stringified = row.inject({}){|hash,(k,v)| hash[k.to_s] = v; hash} #Stringify row keys
        result.push(stringified)
      end
    end

    # the &block is invoked with 4 arguments: local, query[String], params[Hash], result[LookupResult]
    # the result is used as accumulator return variable
    def retrieve_local_data(local, event, &proc)
      result = LookupResult.new()
      if @parameters_specified
        params = prepare_parameters_from_event(event, result)
        if result.parameters_invalid?
          logger.warn? && logger.warn("Parameter field not found in event", :lookup_id => @id, :invalid_parameters => result.invalid_parameters)
          return result
        end
      else
        params = {}
      end
      begin
        logger.debug? && logger.debug("Executing Jdbc query", :lookup_id => @id, :statement => query, :parameters => params)
        proc.call(local, query, params, result)
      rescue => e
        # In theory all exceptions in Sequel should be wrapped in Sequel::Error
        # However, there are cases where other errors can occur - a `SQLTransactionRollbackException`
        # may be thrown during `prepareStatement`. Let's handle these cases here, where we can tag and warn
        # appropriately rather than bubble up and potentially crash the plugin.
        result.failed!
        logger.warn? && logger.warn("Exception when executing Jdbc query", :lookup_id => @id, :exception => e.message, :backtrace => e.backtrace.take(8))
      end
      # if either of: no records or a Sequel exception occurs the payload is
      # empty and the default can be substituted later.
      result
    end

    def process_event(event, result)
      # use deep clone here so other filter function don't taint the payload by reference
      event.set(@target, ::LogStash::Util.deep_clone(result.payload))
    end

    def prepare_parameters_from_event(event, result)
      @symbol_parameters.inject({}) do |hash,(k,v)|
        value = v.fetch(event, result)
        hash[k] = value.is_a?(::LogStash::Timestamp) ? value.time : value
        hash
      end
    end

    def sprintf_or_get(v)
      v.match(/%{([^}]+)}/) ? Sprintfier.new(v) : Getfier.new(v)
    end

    def parse_options
      @query = @options["query"]
      unless @query && @query.is_a?(String)
        @option_errors << "The options for '#{@id}' must include a 'query' string"
      end

      if @options["parameters"] && @options["prepared_parameters"]
        @option_errors << "Can't specify 'parameters' and 'prepared_parameters' in the same lookup"
      else
        @parameters = @options["parameters"]
        @prepared_parameters = @options["prepared_parameters"]
        @parameters_specified = false
        if @parameters
          if !@parameters.is_a?(Hash)
            @option_errors << "The 'parameters' option for '#{@id}' must be a Hash"
          else
            # this is done once per lookup at start, i.e. Sprintfier.new et.al is done once.
            @symbol_parameters = @parameters.inject({}) {|hash,(k,v)| hash[k.to_sym] = sprintf_or_get(v) ; hash }
            # the user might specify an empty hash parameters => {}
            # maybe due to an unparameterised query
            @parameters_specified = !@symbol_parameters.empty?
          end
        elsif @prepared_parameters
          if !@prepared_parameters.is_a?(Array)
            @option_errors << "The 'prepared_parameters' option for '#{@id}' must be an Array"
          elsif @query.count("?") != @prepared_parameters.size
            @option_errors << "The 'prepared_parameters' option for '#{@id}' doesn't match count with query's placeholder"
          else
            #prepare the map @symbol_parameters :n => sprintf_or_get
            hash = {}
            @prepared_parameters.each_with_index {|v,i| hash[:"p#{i}"] = sprintf_or_get(v)}
            @symbol_parameters = hash
            @parameters_specified = !@prepared_parameters.empty?
          end
        end
      end

      default_hash = @options["default_hash"]
      if default_hash && !default_hash.empty?
        @default_result = LookupResult.new()
        @default_result.push(default_hash)
      end

      @use_default = !@default_result.nil?

      @tag_on_failure = @options["tag_on_failure"] || @globals["tag_on_failure"] || []
      @tag_on_default_use = @options["tag_on_default_use"] || @globals["tag_on_default_use"] || []

      @valid = @option_errors.empty?
    end
  end
end end end
