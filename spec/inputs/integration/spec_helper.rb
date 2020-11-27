require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/jdbc"

ENV_TZ = ENV["TZ"]

module LogStash::Inputs::Jdbc::SpecHelpers

  def puts(msg)
    !$VERBOSE.nil? && Kernel.puts(msg)
  end

  def read_yaml(path)
    # "--- !ruby/object:DateTime '2020-11-17 07:56:23.978705000 Z'\n"
    YAML.load(File.read(path))
  end

  def env_zone_utc?
    # we allow (local) testing with skipping the forced ENV['TZ'] = ...
    ENV['TZ'] == "Etc/UTC"
  end

end

RSpec.configure do |config|
  config.include LogStash::Inputs::Jdbc::SpecHelpers
end
