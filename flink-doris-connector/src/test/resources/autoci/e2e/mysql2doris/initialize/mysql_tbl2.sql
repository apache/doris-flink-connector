DROP TABLE IF EXISTS test_e2e_mysql.tbl2;

CREATE TABLE test_e2e_mysql.tbl2 (
    `name` varchar(256) primary key,
    `age` int
);

insert into test_e2e_mysql.tbl2 values ('doris_2',2);