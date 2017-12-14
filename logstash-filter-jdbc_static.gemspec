Gem::Specification.new do |s|
  s.name            = 'logstash-filter-jdbc_static'
  s.version         = '1.0.0'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "This filter executes a SQL query to fetch a SQL query result, store it locally then use a second SQL query to update an event."
  s.description     = "This gem is a Logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/logstash-plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["Elastic"]
  s.email           = 'info@elastic.co'
  s.homepage        = "http://www.elastic.co/guide/en/logstash/current/index.html"
  # to fool jar_dependencies to mimic our other plugin gradle vendor script behaviour
  # the rake vendor task removes jars dir and jars downloaded to it
  s.require_paths   = ["lib", "jars"]

  # Files
  s.files = Dir['lib/**/*','vendor/**/*','spec/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "filter" }

  derby_version = "10.14.1.0"
  s.requirements << "jar 'org.apache.derby:derby', '#{derby_version}'"
  s.requirements << "jar 'org.apache.derby:derbyclient', '#{derby_version}'"
  # we may need 'org.apache.derby:derbynet' in the future, marking this here

  s.add_development_dependency 'jar-dependencies', '~> 0.3'

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_runtime_dependency 'sequel'
  s.add_runtime_dependency 'tzinfo'
  s.add_runtime_dependency 'tzinfo-data'
  s.add_runtime_dependency 'rufus-scheduler'

  s.add_development_dependency 'logstash-devutils'
  s.add_development_dependency "childprocess"
end
