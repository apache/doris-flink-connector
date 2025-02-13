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

INSERT INTO test_doris2doris_source.test_tbl
VALUES
    (
        1,
        TRUE,
        127,
        32767,
        2147483647,
        9223372036854775807,
        123456789012345678901234567890,
        3.14,
        2.7182818284,
        12345.6789,
        '2023-05-22',
        '2023-05-22 12:34:56',
        'A',
        'Example text',
        ['item1', 'item2', 'item3'],
        {'key1': 'value1', 'key2': 'value2'},
        STRUCT('John Doe', 30),
        '{"key": "value"}'
    ),
    (
        2,
        FALSE,
        -128,
        -32768,
        -2147483648,
        -9223372036854775808,
        -123456789012345678901234567890,
        -3.14,
        -2.7182818284,
        -12345.6789,
        '2024-01-01',
        '2024-01-01 00:00:00',
        'B',
        'Another example',
        ['item4', 'item5', 'item6'],
        {'key3': 'value3', 'key4': 'value4'},
        STRUCT('Jane Doe', 25),
        '{"another_key": "another_value"}'
);