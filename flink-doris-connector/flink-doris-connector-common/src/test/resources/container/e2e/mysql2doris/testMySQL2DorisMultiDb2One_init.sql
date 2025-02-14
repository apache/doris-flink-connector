-- tbl1
DROP DATABASE if EXISTS test_e2e_mysql_db1;
CREATE DATABASE if NOT EXISTS test_e2e_mysql_db1;
DROP TABLE IF EXISTS test_e2e_mysql_db1.tbl1;
CREATE TABLE test_e2e_mysql_db1.tbl1 (
`id` int NOT NULL,
`name` varchar(255) DEFAULT NULL,
`age` bigint DEFAULT NULL,
PRIMARY KEY (`id`) USING BTREE
);
insert into test_e2e_mysql_db1.tbl1 values (1,'db1_tb1',18);

DROP DATABASE if EXISTS test_e2e_mysql_db2;
CREATE DATABASE if NOT EXISTS test_e2e_mysql_db2;
DROP TABLE IF EXISTS test_e2e_mysql_db2.tbl1;
CREATE TABLE test_e2e_mysql_db2.tbl1 (
`id` int NOT NULL,
`name` varchar(255) DEFAULT NULL,
`age` bigint DEFAULT NULL,
PRIMARY KEY (`id`) USING BTREE
);
insert into test_e2e_mysql_db2.tbl1 values (2,'db2_tb1',20);

-- tbl2_1 tbl2_2
DROP TABLE IF EXISTS test_e2e_mysql_db1.tbl2_1;
CREATE TABLE test_e2e_mysql_db1.tbl2_1 (
`id` int NOT NULL,
`name` varchar(255) DEFAULT NULL,
`age` bigint DEFAULT NULL,
PRIMARY KEY (`id`) USING BTREE
);
insert into test_e2e_mysql_db1.tbl2_1 values (1,'db1_tb2_1',19);

DROP TABLE IF EXISTS test_e2e_mysql_db1.tbl2_2;
CREATE TABLE test_e2e_mysql_db1.tbl2_2 (
`id` int NOT NULL,
`name` varchar(255) DEFAULT NULL,
`age` bigint DEFAULT NULL,
PRIMARY KEY (`id`) USING BTREE
);
insert into test_e2e_mysql_db1.tbl2_2 values (2,'db1_tb2_2',191);

-- db2
DROP TABLE IF EXISTS test_e2e_mysql_db2.tbl2_1;
CREATE TABLE test_e2e_mysql_db2.tbl2_1 (
`id` int NOT NULL,
`name` varchar(255) DEFAULT NULL,
`age` bigint DEFAULT NULL,
PRIMARY KEY (`id`) USING BTREE
);
insert into test_e2e_mysql_db2.tbl2_1 values (3,'db2_tb2_2',21);

DROP TABLE IF EXISTS test_e2e_mysql_db2.tbl2_2;
CREATE TABLE test_e2e_mysql_db2.tbl2_2 (
`id` int NOT NULL,
`name` varchar(255) DEFAULT NULL,
`age` bigint DEFAULT NULL,
PRIMARY KEY (`id`) USING BTREE
);
insert into test_e2e_mysql_db2.tbl2_2 values (4,'db2_tbl2_2',211);


DROP TABLE IF EXISTS test_e2e_mysql_db2.tbl3;
CREATE TABLE test_e2e_mysql_db2.tbl3 (
`id` int NOT NULL,
`name` varchar(255) DEFAULT NULL,
`age` bigint DEFAULT NULL,
PRIMARY KEY (`id`) USING BTREE
);
insert into test_e2e_mysql_db2.tbl3 values (1,'db2_tb3',22);