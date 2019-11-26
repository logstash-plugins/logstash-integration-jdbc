# encoding: utf-8

# use the rspec --require command line option to have this file evaluated before rspec runs
# it i

GEM_BASE_DIR = ::File.expand_path("../../..", __FILE__)
BASE_DERBY_DIR = ::File.join(GEM_BASE_DIR, "spec", "helpers")
ENV["HOME"] = GEM_BASE_DIR
ENV["TEST_DEBUG"] = "true"
java.lang.System.setProperty("ls.logs", "console")
