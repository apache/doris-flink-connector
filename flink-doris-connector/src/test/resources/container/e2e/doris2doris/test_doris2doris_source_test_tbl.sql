CREATE DATABASE IF NOT EXISTS test_doris2doris_source;

DROP TABLE IF EXISTS test_doris2doris_source.test_tbl;

CREATE TABLE test_doris2doris_source.test_tbl (
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
     `c13` varchar(16),
     `c14` string,
     `c15` Array<String>,
     `c16` Map<String, String>,
     `c17` Struct<name: String, age: int>,
     `c18` JSON,
     `c19` JSON -- doris2.1.0 can not read VARIANT
)
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"light_schema_change" = "true"
);

INSERT INTO test_doris2doris_source.test_tbl VALUES
    (1, true, 127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727,
     3.14, 2.71828, 12345.6789, '2025-03-11', '2025-03-11 12:34:56', 'A', 'Hello, Doris!', 'This is a string',
        ['Alice', 'Bob'], {'key1': 'value1', 'key2': 'value2'}, STRUCT('Tom', 30), '{"key": "value"}', '{"type": "variant", "data": 123}');

INSERT INTO test_doris2doris_source.test_tbl VALUES
    (2, false, -128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728,
     -1.23, 0.0001, -9999.9999, '2024-12-25', '2024-12-25 23:59:59', 'B', 'Doris Test', 'Another string!',
        ['Charlie', 'David'], {'k1': 'v1', 'k2': 'v2'}, STRUCT('Jerry', 25), '{"status": "ok"}', '{"data": [1, 2, 3]}' );

INSERT INTO test_doris2doris_source.test_tbl VALUES
    (3, true, 0, 0, 0, 0, 0,
     0.0, 0.0, 0.0000, '2023-06-15', '2023-06-15 08:00:00', 'C', 'Test Doris', 'Sample text',
        ['Eve', 'Frank'], {'alpha': 'beta'}, STRUCT('Alice', 40), '{"nested": {"key": "value"}}', '{"variant": "test"}');

INSERT INTO test_doris2doris_source.test_tbl VALUES
    (4, NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL);