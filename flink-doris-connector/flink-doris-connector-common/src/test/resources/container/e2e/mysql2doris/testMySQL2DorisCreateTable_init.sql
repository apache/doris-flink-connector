DROP DATABASE if EXISTS test_e2e_mysql;
CREATE DATABASE if NOT EXISTS test_e2e_mysql;
DROP TABLE IF EXISTS test_e2e_mysql.create_tbl_uniq;
CREATE TABLE test_e2e_mysql.create_tbl_uniq (
`id` int NOT NULL,
`name` varchar(255) DEFAULT NULL,
`age` bigint DEFAULT NULL,
PRIMARY KEY (`id`) USING BTREE
);

DROP TABLE IF EXISTS test_e2e_mysql.create_tbl_dup;
CREATE TABLE test_e2e_mysql.create_tbl_dup (
`id` int NOT NULL,
`name` varchar(255) DEFAULT NULL,
`age` bigint DEFAULT NULL
);

DROP TABLE IF EXISTS test_e2e_mysql.create_tbl_from_uniqindex;
CREATE TABLE test_e2e_mysql.create_tbl_from_uniqindex (
`id` int NOT NULL,
`name` varchar(255) DEFAULT NULL,
`age` bigint DEFAULT NULL,
UNIQUE KEY `uniq` (`name`)
);

DROP TABLE IF EXISTS test_e2e_mysql.create_tbl_from_uniqindex2;
CREATE TABLE test_e2e_mysql.create_tbl_from_uniqindex2 (
`id` int DEFAULT NULL,
`name` varchar(255) DEFAULT NULL,
`age` int DEFAULT NULL,
UNIQUE KEY `idname_uniq` (`id`,`name`),
UNIQUE KEY `idage_uniq` (`id`,`age`)
);

DROP TABLE IF EXISTS test_e2e_mysql.create_tbl_from_multiindex;
CREATE TABLE test_e2e_mysql.create_tbl_from_multiindex (
`id` int DEFAULT NULL,
`name` varchar(255) DEFAULT NULL,
`age` int DEFAULT NULL,
UNIQUE KEY `uniq` (`id`),
KEY `normal` (`name`)
);

-- for auto partition table
DROP TABLE IF EXISTS test_e2e_mysql.create_tbl_part_uniq;
CREATE TABLE test_e2e_mysql.create_tbl_part_uniq (
`id` int NOT NULL,
`name` varchar(255) DEFAULT NULL,
`age` int DEFAULT NULL,
`create_dtime` datetime DEFAULT NULL,
PRIMARY KEY (`id`) USING BTREE
);

DROP TABLE IF EXISTS test_e2e_mysql.create_tbl_part_dup;
CREATE TABLE test_e2e_mysql.create_tbl_part_dup (
`id` int NOT NULL,
`name` varchar(255) DEFAULT NULL,
`age` int DEFAULT NULL,
`create_dtime` datetime DEFAULT NULL
);