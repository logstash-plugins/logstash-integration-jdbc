require "logstash/devutils/rake"

task :default do
  system('rake -vT')
end

require 'jars/installer'
task :install_jars do
  # JARS_HOME is used by Jars:: MavenExec.resolve_dependencies_list
  # in `maven[ 'outputDirectory' ] = "#{Jars.home}"`
  # its where the downloaded jars are stashed
  vendor_dir = File.join(Dir.pwd, "vendor", "jar-dependencies", "runtime-jars")
  jars_dir = File.join(Dir.pwd, "jars")
  ENV['JARS_HOME'] = vendor_dir.dup
  Jars::Installer.new.vendor_jars!(false, "jars")
  FileUtils.rm_rf(jars_dir)
end

task :vendor => :install_jars
