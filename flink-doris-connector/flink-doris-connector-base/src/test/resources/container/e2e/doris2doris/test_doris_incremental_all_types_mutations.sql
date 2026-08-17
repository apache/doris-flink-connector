UPDATE test_doris_incremental_all_types_source.test_tbl
SET c1 = NULL,
    c2 = NULL,
    c3 = NULL,
    c4 = NULL,
    c5 = NULL,
    c6 = NULL,
    c7 = NULL,
    c8 = NULL,
    c9 = NULL,
    c10 = NULL,
    c11 = NULL,
    c12 = NULL,
    c13 = NULL,
    c14 = NULL,
    c15 = NULL,
    c16 = NULL,
    c17 = NULL,
    c18 = NULL,
    c19 = NULL
WHERE id = 1;

DELETE FROM test_doris_incremental_all_types_source.test_tbl WHERE id = 2;

INSERT INTO test_doris_incremental_all_types_source.test_tbl VALUES
    (5, TRUE, 127, 32767, 2147483647, 9223372036854775807,
     170141183460469231731687303715884105727, 3.14, 2.71828, 12345.6789,
     '2025-03-11', '2025-03-11 12:34:56', 'A', 'Hello, Doris!',
     'This is a string', ['Alice', 'Bob'], {'key1': 'value1', 'key2': 'value2'},
     STRUCT('Tom', 30), '{"key": "value"}', '{"type": "variant", "data": 123}');
