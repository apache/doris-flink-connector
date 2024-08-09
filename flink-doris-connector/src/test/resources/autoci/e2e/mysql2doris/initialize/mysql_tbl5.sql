DROP TABLE IF EXISTS  test_e2e_mysql.tbl5;

CREATE TABLE test_e2e_mysql.tbl5 (
    `name` varchar(256) primary key,
    `age` int
);

insert into test_e2e_mysql.tbl5 values ('doris_5',5);