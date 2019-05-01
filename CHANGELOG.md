## 1.0.6
  - Fixes connection leak in pipeline reloads by properly disconnecting on plugin close

## 1.0.5
   - [#11](https://github.com/logstash-plugins/logstash-filter-jdbc_streaming/pull/11) Swap out mysql for postgresql for testing

## 1.0.4
   - [JDBC input - #263](https://github.com/logstash-plugins/logstash-input-jdbc/issues/263) Load the driver with the system class loader. Fixes issue loading some JDBC drivers in Logstash 6.2+ 

## 1.0.3
  - Update gemspec summary

## 1.0.2
  - Fix some documentation issues

## 1.0.0
 - Initial release
 - Added LRU + TTL Cache
