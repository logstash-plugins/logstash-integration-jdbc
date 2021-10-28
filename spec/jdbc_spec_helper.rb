module JdbcHelper
  RSpec::Matchers.define :be_a_logstash_timestamp_equivalent_to do |expected|
    expected = LogStash::Timestamp.new(expected) unless expected.kind_of?(LogStash::Timestamp)
    description { "be a LogStash::Timestamp equivalent to #{expected}" }

    match do |actual|
      actual.kind_of?(LogStash::Timestamp) && actual == expected
    end
  end
end