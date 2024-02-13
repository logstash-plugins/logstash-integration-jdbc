# encoding: utf-8

require "sequel"

# prevent Sequel's datetime_class from being modified,
# and ensure behaviour is restored to the library's default
# if something else in the Ruby VM has already changed it.
Sequel.synchronize do
  def Sequel.datetime_class=(klass)
    # noop
  end
  def Sequel.datetime_class
    ::Time
  end
end

# load the named_timezones extension, which will attempt to
# override the global Sequel::datetime_class; for safety,
# we reset it once more.
Sequel.extension(:named_timezones)
Sequel.datetime_class = ::Time
