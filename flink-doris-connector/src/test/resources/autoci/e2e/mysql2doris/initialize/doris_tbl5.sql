DROP TABLE IF EXISTS  test_e2e_mysql.tbl5;

CREATE TABLE test_e2e_mysql.tbl5 (
    `name` varchar(256) primary key,
    `age` int
)
UNIQUE KEY(`name`)
DISTRIBUTED BY HASH(`name`) BUCKETS 1
PROPERTIES (
    "replication_num"="1"
);