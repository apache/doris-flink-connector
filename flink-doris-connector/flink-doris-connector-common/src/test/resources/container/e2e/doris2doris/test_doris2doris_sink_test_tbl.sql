CREATE DATABASE IF NOT EXISTS test_doris2doris_sink;

DROP TABLE IF EXISTS test_doris2doris_sink.test_tbl;

CREATE TABLE test_doris2doris_sink.test_tbl (
      `id` int,
      `c1` boolean,
      `c2` tinyint,
      `c3` smallint,
      `c4` int,
      `c5` bigint,
      `c6` largeint,
      `c7` float,
      `c8` double,
      `c9` decimal(12,4),
      `c10` date,
      `c11` datetime,
      `c12` char(1),
      `c13` varchar(256),
      `c14` Array<String>,
      `c15` Map<String, String>,
      `c16` Struct<name: String, age: int>,
      `c17` JSON
)
    DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"light_schema_change" = "true"
);

