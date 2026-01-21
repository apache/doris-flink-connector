DROP DATABASE if EXISTS test_e2e_mysql_env;
CREATE DATABASE if NOT EXISTS test_e2e_mysql_env;
DROP TABLE IF EXISTS test_e2e_mysql_env.tbl1;
CREATE TABLE test_e2e_mysql_env.tbl1 (
    `name` varchar(256) primary key,
    `age` int
);
insert into test_e2e_mysql_env.tbl1 values ('doris_env_1',1);


DROP TABLE IF EXISTS test_e2e_mysql_env.tbl2;
CREATE TABLE test_e2e_mysql_env.tbl2 (
    `name` varchar(256) primary key,
    `age` int
);
insert into test_e2e_mysql_env.tbl2 values ('doris_env_2',2);


DROP TABLE IF EXISTS test_e2e_mysql_env.tbl3;
CREATE TABLE test_e2e_mysql_env.tbl3 (
    `name` varchar(256) primary key,
    `age` int
);
insert into test_e2e_mysql_env.tbl3 values ('doris_env_3',3);


DROP TABLE IF EXISTS  test_e2e_mysql_env.tbl4;
CREATE TABLE test_e2e_mysql_env.tbl4 (
    `name` varchar(256) primary key,
    `age` int
);
insert into test_e2e_mysql_env.tbl4 values ('doris_env_4',4);

DROP TABLE IF EXISTS  test_e2e_mysql_env.tbl5;
CREATE TABLE test_e2e_mysql_env.tbl5 (
    `name` varchar(256) primary key,
    `age` int
);
insert into test_e2e_mysql_env.tbl5 values ('doris_env_5',5);