# encoding: utf-8
require "childprocess"

module ServerProcessHelpers
  def self.jdbc_static_start_derby_server()
    # client_out = Stud::Temporary.file
    # client_out.sync
    ChildProcess.posix_spawn = true
    cmd = ["java",  "-jar", "#{BASE_DERBY_DIR}/derbyrun.jar", "server",  "start"]
    process = ChildProcess.build(*cmd)
    process.start

    sleep(0.1)
  end

  def self.jdbc_static_stop_derby_server(test_db)
    cmd = ["java",  "-jar", "#{BASE_DERBY_DIR}/derbyrun.jar", "server",  "shutdown"]
    process = ChildProcess.build(*cmd)
    ChildProcess.posix_spawn = true
    process.start
    process.wait
    `rm -rf #{::File.join(GEM_BASE_DIR, test_db)}`
  end
end
