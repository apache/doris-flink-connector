CREATE DATABASE if NOT EXISTS test_e2e_mysql;
DROP TABLE IF EXISTS test_e2e_mysql.tbl1;
CREATE TABLE test_e2e_mysql.tbl1 (
    `name` varchar(256) primary key,
    `age` int
);
insert into test_e2e_mysql.tbl1 values ('doris_1',1);


DROP TABLE IF EXISTS test_e2e_mysql.tbl2;
CREATE TABLE test_e2e_mysql.tbl2 (
    `name` varchar(256) primary key,
    `age` int
);
insert into test_e2e_mysql.tbl2 values ('doris_2',2);


DROP TABLE IF EXISTS test_e2e_mysql.tbl3;
CREATE TABLE test_e2e_mysql.tbl3 (
    `name` varchar(256) primary key,
    `age` int
);
insert into test_e2e_mysql.tbl3 values ('doris_3',3);


DROP TABLE IF EXISTS  test_e2e_mysql.tbl4;
CREATE TABLE test_e2e_mysql.tbl4 (
    `name` varchar(256) primary key,
    `age` int
);


DROP TABLE IF EXISTS  test_e2e_mysql.tbl5;
CREATE TABLE test_e2e_mysql.tbl5 (
    `name` varchar(256) primary key,
    `age` int
);
insert into test_e2e_mysql.tbl5 values ('doris_5',5);