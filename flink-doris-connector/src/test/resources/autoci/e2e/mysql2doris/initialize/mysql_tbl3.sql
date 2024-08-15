DROP TABLE IF EXISTS test_e2e_mysql.tbl3;

CREATE TABLE test_e2e_mysql.tbl3 (
    `name` varchar(256) primary key,
    `age` int
);

insert into test_e2e_mysql.tbl3 values ('doris_3',3);