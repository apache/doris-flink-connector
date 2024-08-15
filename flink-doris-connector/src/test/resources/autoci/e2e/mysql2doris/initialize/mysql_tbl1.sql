DROP TABLE IF EXISTS test_e2e_mysql.tbl1;

CREATE TABLE test_e2e_mysql.tbl1 (
    `name` varchar(256) primary key,
    `age` int
);

insert into test_e2e_mysql.tbl1 values ('doris_1',1);