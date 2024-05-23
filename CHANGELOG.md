## 5.4.11
  - Fixes an issue in which any one instance of a JDBC input plugin using `jdbc_default_timezone` changes the behaviour of plugin instances that do _not_ use `jdbc_default_timezone`, ensuring that timezone offsets remain consistent for each instance of the plugin _as configured_ [#151](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/151)
  - Fixes an exception that could occur while reloading `jdbc_static` databases when the underlying connection to the remote has been broken [#165](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/165)

## 5.4.10
  - Adds retry mechanism when checkout Derby from SVN repository [#158](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/158)
  - [DOC] add known limitations and settings for connection issue [#167](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/167)

## 5.4.9
  - Fix Derby missed driver classes when built locally for version 10.15 [#160](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/160)

## 5.4.8
  - Update Derby with locally built 10.15.2.1 version [#155](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/155) 

## 5.4.7
  - Update sequel version to >= 5.74.0. It fixes the generic jdbc adapter to properly handle disconnect errors [#153](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/153)

## 5.4.6
  - Update sequel version to >= 5.73.0. The ibmdb and jdbc/db2 adapters were fixed to properly handle disconnect errors, removing the related connection from the pool [#144](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/144)

## 5.4.5
  - Pin sequel to < 5.72.0 due to ruby/bigdecimal#169 [#141](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/141)

## 5.4.4
  - Fix: adaptations for JRuby 9.4 [#125](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/125)

## 5.4.3
  - Fix crash when metadata file can't be deleted after moving under path.data [#136](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/136)

## 5.4.2
  - Doc: described default_hash and tag_on_default_use interaction filter plugin [#122](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/122)
  - Added new settings `statement_retry_attempts` and `statement_retry_attempts_wait_time` for retry of failed sql statement execution [#123](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/123)

## 5.4.1
  - Bugfix leak which happened in creating a new Database pool for every query. The pool is now crated on registration and closed on plugin's `stop` [#119](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/119)
 
## 5.4.0
  - Ambiguous Timestamp Support [#92](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/92)
    - FIX: when encountering an ambiguous timestamp, the JDBC Input no longer crashes
    - Added support for disambiguating timestamps in daylight saving time (DST) overlap periods

## 5.3.0
  - Refactor: start using scheduler mixin [#110](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/110)

## 5.2.6
  - Fix: change default path of 'last_run_metadata_path' to be rooted in the LS data.path folder and not in $HOME [#106](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/106)

## 5.2.5
  - Fix: do not execute more queries with debug logging [#109](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/109)

## 5.2.4
  - Fix: compatibility with all (>= 3.0) rufus-scheduler versions [#97](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/97) 

## 5.2.3
  - Performance: avoid contention on scheduler execution [#103](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/103)

## 5.2.2
  - Feat: name scheduler threads + redirect error logging [#102](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/102)

## 5.2.1
  - Refactor: isolate paginated normal statement algorithm in a separate handler [#101](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/101)

## 5.2.0
  - Added `jdbc_paging_mode` option to choose if use `explicit` pagination in statements and avoid the initial count 
    query or use `auto` to delegate to the underlying library [#95](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/95)

## 5.1.10
  - Refactor: to explicit Java (driver) class name loading [#96](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/96),
    the change is expected to provide a more robust fix for the driver loading issue [#83](https://github.com/logstash-plugins/logstash-integration-jdbc/issues/83).

    NOTE: a fatal driver error will no longer keep reloading the pipeline and now leads to a system exit. 

  - Fix: regression due returning the Java driver class [#98](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/98)

## 5.1.9 (yanked)
  - Refactor: to explicit Java (driver) class name loading [#96](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/96),
    the change is expected to provide a more robust fix for the driver loading issue [#83](https://github.com/logstash-plugins/logstash-integration-jdbc/issues/83).

## 5.1.8
  - Fix the blocking pipeline reload and shutdown when connectivity issues happen [#85](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/85)

## 5.1.7
  - Normalize jdbc_driver_class loading to support any top-level java packages [#86](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/86)

## 5.1.6
  - Fix, serialize the JDBC driver loading steps to avoid concurrency issues [#84](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/84)

## 5.1.5
  - Refined ECS support [#82](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/82)
    - Uses shared `target` guidance when ECS compatibility is enabled
    - Uses Logstash's EventFactory instead of instantiating events directly

## 5.1.4
  - [DOC] Update filter-jdbc_static doc to describe ECS compatibility [#79](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/79)

## 5.1.3
  - Improve robustness when handling errors from `sequel` library in jdbc static and streaming
    filters [#78](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/78)

## 5.1.2
  -  Fix `prepared_statement_bind_values` in streaming filter to resolve nested event's fields [#76](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/76)

## 5.1.1
  - [DOC] Changed docs to indicate that logstash-jdbc-static requires local_table [#56](https://github.com/logstash-plugins/logstash-integration-jdbc/pull/56). Fixes [#55](https://github.com/logstash-plugins/logstash-integration-jdbc/issues/55).

## 5.1.0
  - Added `target` option to JDBC input, allowing the row columns to target a specific field instead of being expanded 
    at the root of the event. This allows the input to play nicer with the Elastic Common Schema when 
    the input does not follow the schema. [#69](https://github.com/logstash-plugins/logstash-integration-jdbc/issues/69)
    
  - Added `target` to JDBC filter static `local_lookups` to verify it's properly valued when ECS is enabled. 
    [#71](https://github.com/logstash-plugins/logstash-integration-jdbc/issues/71)

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
 
