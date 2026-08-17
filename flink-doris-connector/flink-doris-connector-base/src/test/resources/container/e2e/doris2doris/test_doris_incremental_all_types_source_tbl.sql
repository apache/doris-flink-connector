CREATE DATABASE IF NOT EXISTS test_doris_incremental_all_types_source;

DROP TABLE IF EXISTS test_doris_incremental_all_types_source.test_tbl;

CREATE TABLE test_doris_incremental_all_types_source.test_tbl (
    `id` INT,
    `c1` BOOLEAN,
    `c2` TINYINT,
    `c3` SMALLINT,
    `c4` INT,
    `c5` BIGINT,
    `c6` LARGEINT,
    `c7` FLOAT,
    `c8` DOUBLE,
    `c9` DECIMAL(12, 4),
    `c10` DATE,
    `c11` DATETIME,
    `c12` CHAR(1),
    `c13` VARCHAR(16),
    `c14` STRING,
    `c15` ARRAY<STRING>,
    `c16` MAP<STRING, STRING>,
    `c17` STRUCT<name: STRING, age: INT>,
    `c18` JSON,
    `c19` JSON
)
UNIQUE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "light_schema_change" = "true",
    "enable_unique_key_merge_on_write" = "true",
    "binlog.enable" = "true",
    "binlog.format" = "ROW",
    "binlog.need_historical_value" = "true"
);
