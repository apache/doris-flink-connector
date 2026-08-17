INSERT INTO test_doris_incremental_all_types_source.test_tbl VALUES
    (1, TRUE, 127, 32767, 2147483647, 9223372036854775807,
     170141183460469231731687303715884105727, 3.14, 2.71828, 12345.6789,
     '2025-03-11', '2025-03-11 12:34:56', 'A', 'Hello, Doris!',
     'This is a string', ['Alice', 'Bob'], {'key1': 'value1', 'key2': 'value2'},
     STRUCT('Tom', 30), '{"key": "value"}', '{"type": "variant", "data": 123}');

INSERT INTO test_doris_incremental_all_types_source.test_tbl VALUES
    (2, FALSE, -128, -32768, -2147483648, -9223372036854775808,
     -170141183460469231731687303715884105728, -1.23, 0.0001, -9999.9999,
     '2024-12-25', '2024-12-25 23:59:59', 'B', 'Doris Test',
     'Another string!', ['Charlie', 'David'], {'k1': 'v1', 'k2': 'v2'},
     STRUCT('Jerry', 25), '{"status": "ok"}', '{"data": [1, 2, 3]}');

INSERT INTO test_doris_incremental_all_types_source.test_tbl VALUES
    (3, TRUE, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0000,
     '2023-06-15', '2023-06-15 08:00:00', 'C', 'Test Doris', 'Sample text',
     ['Eve', 'Frank'], {'alpha': 'beta'}, STRUCT('Alice', 40),
     '{"nested": {"key": "value"}}', '{"variant": "test"}');

INSERT INTO test_doris_incremental_all_types_source.test_tbl VALUES
    (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
