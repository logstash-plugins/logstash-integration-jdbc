## 5.0.7
  - Feat: try hard to log Java cause (chain) [#62](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/62)

    This allows seeing a full trace from the JDBC driver in case of connection errors. 

  - Refactored Lookup used in jdbc_streaming and jdbc_static to avoid code duplication. [#59](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/59)

## 5.0.6
  - DOC:Replaced plugin_header file with plugin_header-integration file. [#40](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/40)

## 5.0.5
  - Fixed user sequel_opts not being passed down properly [#37](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/37)
  - Refactored jdbc_streaming to share driver loading, so the fixes from the jdbc plugin also effect jdbc_streaming

## 5.0.4
  - Fixed issue where JDBC Drivers that don't correctly register with Java's DriverManager fail to load (such as Sybase) [#34](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/34)

## 5.0.3
  - Fixed issue where a lost connection to the database can cause errors when using prepared statements with the scheduler [#25](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/25)

## 5.0.2
  - Fixed potential resource leak by ensuring scheduler is shutdown when a pipeline encounter an error during the running [#28](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/28)

## 5.0.1
  - Fixed tracking_column regression with Postgresql Numeric types [#17](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/17)
  - Fixed driver loading when file not accessible [#15](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/15)

## 5.0.0
  - Initial Release of JDBC Integration Plugin, incorporating [logstash-input-jdbc](https://github.com/logstash-plugins/logstash-input-jdbc), [logstash-filter-jdbc_streaming](https://github.com/logstash-plugins/logstash-filter-jdbc_streaming) and
    [logstash-filter-jdbc_static](https://github.com/logstash-plugins/logstash-filter-jdbc_static)
  - For Changelog of individual plugins, see:
    - [JBDC Input version 4.3.19](https://github.com/logstash-plugins/logstash-input-jdbc/blob/v4.3.19/CHANGELOG.md)
    - [JDBC Static filter version 1.1.0](https://github.com/logstash-plugins/logstash-filter-jdbc_static/blob/v1.1.0/CHANGELOG.md)
    - [JDBC Streaming filter version 1.0.10](https://github.com/logstash-plugins/logstash-filter-jdbc_streaming/blob/v1.0.10/CHANGELOG.md)
 