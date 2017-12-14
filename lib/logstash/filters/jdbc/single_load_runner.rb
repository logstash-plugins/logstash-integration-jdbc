require_relative 'db_object'

module LogStash module Filters module Jdbc
  class SingleLoadRunner

    attr_reader :local, :loaders, :preloaders

    def initialize(local, loaders, preloaders)
      @local = local
      @loaders = loaders
      @preloaders = []
      preloaders.map do |pre|
        dbo = DbObject.new(pre)
        @preloaders << dbo
        hash = dbo.as_temp_table_opts
        _dbo = DbObject.new(hash)
        @preloaders << _dbo if _dbo.valid?
      end
      @preloaders.sort!
    end

    def initial_load
      do_preload
      local.populate_all(loaders)
    end

    def repeated_load
    end

    def call
      repeated_load
    end

    private

    def do_preload
      preloaders.each do |db_object|
        local.build_db_object(db_object)
      end
    end
  end

end end end
