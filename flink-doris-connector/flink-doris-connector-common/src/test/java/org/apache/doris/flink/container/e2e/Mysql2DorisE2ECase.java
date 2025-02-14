// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.container.e2e;

import org.apache.doris.flink.container.AbstractE2EService;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.tools.cdc.DatabaseSyncConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class Mysql2DorisE2ECase extends AbstractE2EService {
    private static final Logger LOG = LoggerFactory.getLogger(Mysql2DorisE2ECase.class);
    private static final String DATABASE = "test_e2e_mysql";
    private static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS " + DATABASE;
    private static final String DROP_DATABASE = "DROP DATABASE IF EXISTS " + DATABASE;
    private static final String MYSQL_CONF = "--" + DatabaseSyncConfig.MYSQL_CONF;

    @Before
    public void setUp() throws InterruptedException {
        LOG.info("Mysql2DorisE2ECase attempting to acquire semaphore.");
        SEMAPHORE.acquire();
        LOG.info("Mysql2DorisE2ECase semaphore acquired.");
    }

    private List<String> setMysql2DorisDefaultConfig(List<String> argList) {
        // set default mysql config
        argList.add(MYSQL_CONF);
        argList.add(HOSTNAME + "=" + getMySQLInstanceHost());
        argList.add(MYSQL_CONF);
        argList.add(PORT + "=" + getMySQLQueryPort());
        argList.add(MYSQL_CONF);
        argList.add(USERNAME + "=" + getMySQLUsername());
        argList.add(MYSQL_CONF);
        argList.add(PASSWORD + "=" + getMySQLPassword());
        argList.add(MYSQL_CONF);
        argList.add("server-time-zone=UTC");

        setSinkConfDefaultConfig(argList);
        return argList;
    }

    private void startMysql2DorisJob(String jobName, String resourcePath) {
        LOG.info("start a mysql to doris job. jobName={}, resourcePath={}", jobName, resourcePath);
        List<String> argList = ContainerUtils.parseFileArgs(resourcePath);
        String[] args = setMysql2DorisDefaultConfig(argList).toArray(new String[0]);
        submitE2EJob(jobName, args);
    }

    private void initMysqlEnvironment(String sourcePath) {
        LOG.info("Initializing MySQL environment.");
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(), LOG, ContainerUtils.parseFileContentSQL(sourcePath));
    }

    private void initDorisEnvironment() {
        LOG.info("Initializing Doris environment.");
        ContainerUtils.executeSQLStatement(getDorisQueryConnection(), LOG, DROP_DATABASE);
    }

    private void initEnvironment(String jobName, String mysqlSourcePath) {
        LOG.info(
                "start to init mysql to doris environment. jobName={}, mysqlSourcePath={}",
                jobName,
                mysqlSourcePath);
        initMysqlEnvironment(mysqlSourcePath);
        initDorisEnvironment();
    }

    @Test
    public void testMySQL2Doris() throws Exception {
        String jobName = "testMySQL2Doris";
        String resourcePath = "container/e2e/mysql2doris/testMySQL2Doris.txt";
        initEnvironment(jobName, "container/e2e/mysql2doris/testMySQL2Doris_init.sql");
        startMysql2DorisJob(jobName, resourcePath);

        // wait 2 times checkpoint
        Thread.sleep(20000);
        LOG.info("Start to verify init result.");
        List<String> expected = Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3", "doris_5,5");
        String sql1 =
                "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 union all select * from test_e2e_mysql.tbl5) res order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, sql1, 2);

        // add incremental data
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "insert into test_e2e_mysql.tbl1 values ('doris_1_1',10)",
                "insert into test_e2e_mysql.tbl2 values ('doris_2_1',11)",
                "insert into test_e2e_mysql.tbl3 values ('doris_3_1',12)",
                "update test_e2e_mysql.tbl1 set age=18 where name='doris_1'",
                "delete from test_e2e_mysql.tbl2 where name='doris_2'");
        Thread.sleep(20000);

        LOG.info("Start to verify incremental data result.");
        List<String> expected2 =
                Arrays.asList(
                        "doris_1,18", "doris_1_1,10", "doris_2_1,11", "doris_3,3", "doris_3_1,12");
        String sql2 =
                "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 ) res order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected2, sql2, 2);

        // mock schema change
        LOG.info("start to schema change in mysql.");
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "alter table test_e2e_mysql.tbl1 add column c1 varchar(128)",
                "alter table test_e2e_mysql.tbl1 drop column age");
        Thread.sleep(10000);
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "insert into test_e2e_mysql.tbl1  values ('doris_1_1_1','c1_val')");
        Thread.sleep(20000);
        LOG.info("verify tal1 schema change.");
        List<String> schemaChangeExpected =
                Arrays.asList("doris_1,null", "doris_1_1,null", "doris_1_1_1,c1_val");
        String schemaChangeSql = "select * from test_e2e_mysql.tbl1 order by 1";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, schemaChangeExpected, schemaChangeSql, 2);
        cancelE2EJob(jobName);
    }

    @Test
    public void testAutoAddTable() throws InterruptedException {
        String jobName = "testAutoAddTable";
        initEnvironment(jobName, "container/e2e/mysql2doris/testAutoAddTable_init.sql");
        startMysql2DorisJob(jobName, "container/e2e/mysql2doris/testAutoAddTable.txt");

        // wait 2 times checkpoint
        Thread.sleep(20000);
        LOG.info("Start to verify init result.");
        List<String> expected = Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3", "doris_5,5");
        String sql1 =
                "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 union all select * from test_e2e_mysql.tbl5) res order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, sql1, 2);

        // auto add table
        LOG.info("starting to create auto_add table.");
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "CREATE TABLE test_e2e_mysql.auto_add ( \n"
                        + "`name` varchar(256) primary key,\n"
                        + "`age` int\n"
                        + ")",
                "insert into test_e2e_mysql.auto_add  values ('doris_4_1',4)",
                "insert into test_e2e_mysql.auto_add  values ('doris_4_2',4)");
        Thread.sleep(20000);
        List<String> autoAddResult = Arrays.asList("doris_4_1,4", "doris_4_2,4");
        String autoAddSql = "select * from test_e2e_mysql.auto_add order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, autoAddResult, autoAddSql, 2);

        // incremental data
        LOG.info("starting to increment data.");
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "insert into test_e2e_mysql.tbl1 values ('doris_1_1',10)",
                "insert into test_e2e_mysql.tbl2 values ('doris_2_1',11)",
                "insert into test_e2e_mysql.tbl3 values ('doris_3_1',12)",
                "update test_e2e_mysql.tbl1 set age=18 where name='doris_1'",
                "delete from test_e2e_mysql.tbl2 where name='doris_2'",
                "insert into test_e2e_mysql.auto_add values ('doris_4_3',43)",
                "delete from test_e2e_mysql.auto_add where name='doris_4_2'",
                "update test_e2e_mysql.auto_add set age=41 where name='doris_4_1'");
        Thread.sleep(20000);
        List<String> incrementDataExpected =
                Arrays.asList(
                        "doris_1,18",
                        "doris_1_1,10",
                        "doris_2_1,11",
                        "doris_3,3",
                        "doris_3_1,12",
                        "doris_4_1,41",
                        "doris_4_3,43");
        String incrementDataSql =
                "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 union all select * from test_e2e_mysql.auto_add) res order by 1";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, incrementDataExpected, incrementDataSql, 2);

        // schema change
        LOG.info("starting to mock schema change.");
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "alter table test_e2e_mysql.auto_add add column c1 varchar(128)",
                "alter table test_e2e_mysql.auto_add drop column age",
                "insert into test_e2e_mysql.auto_add values ('doris_4_4','c1_val')");
        Thread.sleep(20000);
        List<String> schemaChangeExpected =
                Arrays.asList("doris_4_1,null", "doris_4_3,null", "doris_4_4,c1_val");
        String schemaChangeSql = "select * from test_e2e_mysql.auto_add order by 1";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, schemaChangeExpected, schemaChangeSql, 2);
        cancelE2EJob(jobName);
    }

    @Test
    public void testMySQL2DorisSQLParse() throws Exception {
        String jobName = "testMySQL2DorisSQLParse";
        String resourcePath = "container/e2e/mysql2doris/testMySQL2DorisSQLParse.txt";
        initEnvironment(jobName, "container/e2e/mysql2doris/testMySQL2DorisSQLParse_init.sql");
        startMysql2DorisJob(jobName, resourcePath);

        // wait 2 times checkpoint
        Thread.sleep(20000);
        LOG.info("Start to verify init result.");
        List<String> expected = Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3", "doris_5,5");
        String sql1 =
                "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 union all select * from test_e2e_mysql.tbl5) res order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, sql1, 2);

        // add incremental data
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "insert into test_e2e_mysql.tbl1 values ('doris_1_1',10)",
                "insert into test_e2e_mysql.tbl2 values ('doris_2_1',11)",
                "insert into test_e2e_mysql.tbl3 values ('doris_3_1',12)",
                "update test_e2e_mysql.tbl1 set age=18 where name='doris_1'",
                "delete from test_e2e_mysql.tbl2 where name='doris_2'");
        Thread.sleep(20000);

        LOG.info("Start to verify incremental data result.");
        List<String> expected2 =
                Arrays.asList(
                        "doris_1,18", "doris_1_1,10", "doris_2_1,11", "doris_3,3", "doris_3_1,12");
        String sql2 =
                "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 ) res order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected2, sql2, 2);

        // mock schema change
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "alter table test_e2e_mysql.tbl1 add column c1 varchar(128)",
                "alter table test_e2e_mysql.tbl1 drop column age");
        Thread.sleep(10000);
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "insert into test_e2e_mysql.tbl1  values ('doris_1_1_1','c1_val')");
        Thread.sleep(20000);
        LOG.info("verify tal1 schema change.");
        List<String> schemaChangeExpected =
                Arrays.asList("doris_1,null", "doris_1_1,null", "doris_1_1_1,c1_val");
        String schemaChangeSql = "select * from test_e2e_mysql.tbl1 order by 1";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, schemaChangeExpected, schemaChangeSql, 2);

        // mock create table
        LOG.info("start to create table in mysql.");
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "CREATE TABLE test_e2e_mysql.add_tbl (\n"
                        + "    `name` varchar(256) primary key,\n"
                        + "    `age` int\n"
                        + ");",
                "insert into test_e2e_mysql.add_tbl  values ('doris_1',1)",
                "insert into test_e2e_mysql.add_tbl  values ('doris_2',2)",
                "insert into test_e2e_mysql.add_tbl  values ('doris_3',3)");
        Thread.sleep(20000);
        List<String> createTableExpected = Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3");
        String createTableSql = "select * from test_e2e_mysql.add_tbl order by 1";
        ContainerUtils.checkResult(
                getDorisQueryConnection(), LOG, createTableExpected, createTableSql, 2);
        cancelE2EJob(jobName);
    }

    @Test
    public void testMySQL2DorisByDefault() throws Exception {
        String jobName = "testMySQL2DorisByDefault";
        initEnvironment(jobName, "container/e2e/mysql2doris/testMySQL2DorisByDefault_init.sql");
        startMysql2DorisJob(jobName, "container/e2e/mysql2doris/testMySQL2DorisByDefault.txt");

        // wait 2 times checkpoint
        Thread.sleep(20000);
        LOG.info("Start to verify init result.");
        List<String> expected = Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3", "doris_5,5");
        String sql1 =
                "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 union all select * from test_e2e_mysql.tbl5) res order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, sql1, 2);

        // add incremental data
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "insert into test_e2e_mysql.tbl1 values ('doris_1_1',10)",
                "insert into test_e2e_mysql.tbl2 values ('doris_2_1',11)",
                "insert into test_e2e_mysql.tbl3 values ('doris_3_1',12)",
                "update test_e2e_mysql.tbl1 set age=18 where name='doris_1'",
                "delete from test_e2e_mysql.tbl2 where name='doris_2'");
        Thread.sleep(20000);

        LOG.info("Start to verify incremental data result.");
        List<String> expected2 =
                Arrays.asList(
                        "doris_1,18", "doris_1_1,10", "doris_2_1,11", "doris_3,3", "doris_3_1,12");
        String sql2 =
                "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 ) res order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected2, sql2, 2);
        cancelE2EJob(jobName);
    }

    @Test
    public void testMySQL2DorisEnableDelete() throws Exception {
        String jobName = "testMySQL2DorisEnableDelete";
        initEnvironment(jobName, "container/e2e/mysql2doris/testMySQL2DorisEnableDelete_init.sql");
        startMysql2DorisJob(jobName, "container/e2e/mysql2doris/testMySQL2DorisEnableDelete.txt");

        // wait 2 times checkpoint
        Thread.sleep(20000);
        LOG.info("Start to verify init result.");
        List<String> initExpected =
                Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3", "doris_5,5");
        String sql1 =
                "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 union all select * from test_e2e_mysql.tbl5) res order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, initExpected, sql1, 2);

        // add incremental data
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "insert into test_e2e_mysql.tbl1 values ('doris_1_1',10)",
                "insert into test_e2e_mysql.tbl2 values ('doris_2_1',11)",
                "insert into test_e2e_mysql.tbl3 values ('doris_3_1',12)",
                "update test_e2e_mysql.tbl1 set age=18 where name='doris_1'",
                "delete from test_e2e_mysql.tbl2 where name='doris_2'",
                "delete from test_e2e_mysql.tbl3 where name='doris_3'",
                "delete from test_e2e_mysql.tbl5 where name='doris_5'");

        Thread.sleep(20000);
        List<String> expected =
                Arrays.asList(
                        "doris_1,18",
                        "doris_1_1,10",
                        "doris_2,2",
                        "doris_2_1,11",
                        "doris_3,3",
                        "doris_3_1,12",
                        "doris_5,5");
        String sql =
                "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 union all select * from test_e2e_mysql.tbl5) res order by 1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, expected, sql, 2);
        cancelE2EJob(jobName);
    }

    @Test
    public void testMySQL2DorisCreateTableOnly() throws Exception {
        String jobName = "testMySQL2DorisCreateTableOnly";
        initEnvironment(jobName, "container/e2e/mysql2doris/testMySQL2DorisCreateTable_init.sql");
        startMysql2DorisJob(jobName, "container/e2e/mysql2doris/testMySQL2DorisCreateTable.txt");

        String createTblSQL = getCreateTableSQL(DATABASE, "create_tbl_uniq");
        Assert.assertTrue(createTblSQL.contains("UNIQUE KEY(`id`)"));
        Assert.assertTrue(createTblSQL.contains("BUCKETS 10"));

        createTblSQL = getCreateTableSQL(DATABASE, "create_tbl_dup");
        Assert.assertTrue(createTblSQL.contains("DUPLICATE KEY(`id`, `name`)"));
        Assert.assertTrue(createTblSQL.contains("BUCKETS AUTO"));

        createTblSQL = getCreateTableSQL(DATABASE, "create_tbl_from_uniqindex");
        Assert.assertTrue(createTblSQL.contains("UNIQUE KEY(`name`)"));
        Assert.assertTrue(createTblSQL.contains("BUCKETS 30"));

        createTblSQL = getCreateTableSQL(DATABASE, "create_tbl_from_uniqindex2");
        Assert.assertTrue(
                createTblSQL.contains("UNIQUE KEY(`id`, `name`)")
                        || createTblSQL.contains("UNIQUE KEY(`id`, `age`)"));
        Assert.assertTrue(createTblSQL.contains("BUCKETS 30"));

        createTblSQL = getCreateTableSQL(DATABASE, "create_tbl_from_multiindex");
        Assert.assertTrue(createTblSQL.contains("UNIQUE KEY(`id`)"));
        Assert.assertTrue(createTblSQL.contains("BUCKETS AUTO"));

        /*
        The auto partition behavior of doris 2.1.0 to 2.1.4 has changed, temporarily skipped
        createTblSQL = getCreateTableSQL(DATABASE, "create_tbl_part_uniq");
        Assert.assertTrue(createTblSQL.contains("UNIQUE KEY(`id`, `create_dtime`)"));
        Assert.assertTrue(createTblSQL.contains("BUCKETS AUTO"));

        createTblSQL = getCreateTableSQL(DATABASE, "create_tbl_part_dup");
        Assert.assertTrue(createTblSQL.contains("DUPLICATE KEY(`id`, `create_dtime`, `name`)"));
        Assert.assertTrue(createTblSQL.contains("BUCKETS AUTO"));
         */
    }

    private String getCreateTableSQL(String database, String table) throws Exception {
        Statement statement = getDorisQueryConnection().createStatement();
        ResultSet resultSet =
                statement.executeQuery(String.format("SHOW CREATE TABLE %s.%s", database, table));
        while (resultSet.next()) {
            String createTblSql = resultSet.getString(2);
            LOG.info("Create table sql: {}", createTblSql.replace("\n", ""));
            return createTblSql;
        }
        throw new RuntimeException("Table not exist " + table);
    }

    @Test
    public void testMySQL2DorisMultiDatabaseSync() throws Exception {
        String jobName = "testMySQL2DorisMultiDatabaseSync";
        ContainerUtils.executeSQLStatement(
                getDorisQueryConnection(), LOG, "DROP DATABASE IF EXISTS test_e2e_mysql_db1");
        ContainerUtils.executeSQLStatement(
                getDorisQueryConnection(), LOG, "DROP DATABASE IF EXISTS test_e2e_mysql_db2");
        initEnvironment(jobName, "container/e2e/mysql2doris/testMySQL2DorisMultiDbSync_init.sql");
        startMysql2DorisJob(jobName, "container/e2e/mysql2doris/testMySQL2DorisMultiDbSync.txt");

        // wait 3 times checkpoint
        Thread.sleep(30000);
        LOG.info("Start to verify init result.");
        List<String> initExpected1 = Arrays.asList("1,db1_tb1,18");
        String sql1 = "SELECT * FROM test_e2e_mysql_db1.tbl1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, initExpected1, sql1, 3, false);

        List<String> initExpected2 = Arrays.asList("1,db1_tb2,19");
        String sql2 = "SELECT * FROM test_e2e_mysql_db1.tbl2";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, initExpected2, sql2, 3, false);

        List<String> initExpected3 = Arrays.asList("1,db2_tb1,20");
        String sql3 = "SELECT * FROM test_e2e_mysql_db2.tbl1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, initExpected3, sql3, 3, false);

        List<String> initExpected4 = Arrays.asList("1,db2_tb2,21");
        String sql4 = "SELECT * FROM test_e2e_mysql_db2.tbl2";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, initExpected4, sql4, 3, false);

        List<String> initExpected5 = Arrays.asList("1,db2_tb3,22");
        String sql5 = "SELECT * FROM test_e2e_mysql_db2.tbl3";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, initExpected5, sql5, 3, false);

        // add incremental data
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "insert into test_e2e_mysql_db1.tbl1 values (2,'db1_tb1',180)",
                "insert into test_e2e_mysql_db1.tbl2 values (2,'db1_tb2',190)",
                "insert into test_e2e_mysql_db2.tbl1 values (2,'db2_tb1',200)",
                "insert into test_e2e_mysql_db2.tbl2 values (2,'db2_tb2',210)",
                "insert into test_e2e_mysql_db2.tbl3 values (2,'db2_tb3',220)");

        Thread.sleep(20000);
        List<String> incrExpected1 = Arrays.asList("1,db1_tb1,18", "2,db1_tb1,180");
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, incrExpected1, sql1, 3, false);

        List<String> incrExpected2 = Arrays.asList("1,db1_tb2,19", "2,db1_tb2,190");
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, incrExpected2, sql2, 3, false);

        List<String> incrExpected3 = Arrays.asList("1,db2_tb1,20", "2,db2_tb1,200");
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, incrExpected3, sql3, 3, false);

        List<String> incrExpected4 = Arrays.asList("1,db2_tb2,21", "2,db2_tb2,210");
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, incrExpected4, sql4, 3, false);

        List<String> incrExpected5 = Arrays.asList("1,db2_tb3,22", "2,db2_tb3,220");
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, incrExpected5, sql5, 3, false);

        cancelE2EJob(jobName);
    }

    /**
     * Separate databases and tables to write to the same database and table
     *
     * @throws Exception
     */
    @Test
    public void testMySQL2DorisMultiDatabase2OneSync() throws Exception {
        String jobName = "testMySQL2DorisMultiDatabase2OneSync";
        initEnvironment(jobName, "container/e2e/mysql2doris/testMySQL2DorisMultiDb2One_init.sql");
        startMysql2DorisJob(jobName, "container/e2e/mysql2doris/testMySQL2DorisMultiDb2One.txt");

        // wait 3 times checkpoint
        Thread.sleep(30000);
        LOG.info("Start to verify init result.");
        List<String> initExpected = Arrays.asList("1,db1_tb1,18", "2,db2_tb1,20");
        String sql1 = "SELECT * FROM test_e2e_mysql.tbl1";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, initExpected, sql1, 3, false);

        List<String> initExpected2 =
                Arrays.asList(
                        "1,db1_tb2_1,19", "2,db1_tb2_2,191", "3,db2_tb2_2,21", "4,db2_tbl2_2,211");
        String sql2 = "SELECT * FROM test_e2e_mysql.tbl2_merge";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, initExpected2, sql2, 3, false);

        List<String> initExpected3 = Arrays.asList("1,db2_tb3,22");
        String sql3 = "SELECT * FROM test_e2e_mysql.tbl3";
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, initExpected3, sql3, 3, false);

        // add incremental data
        ContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "insert into test_e2e_mysql_db1.tbl1 values (3,'db1_tb1',180)",
                "insert into test_e2e_mysql_db2.tbl1 values (4,'db2_tb1',200)",
                "insert into test_e2e_mysql_db1.tbl2_1 values (5,'db1_tb2_1',1901)",
                "insert into test_e2e_mysql_db1.tbl2_2 values (6,'db1_tb2_2',1902)",
                "insert into test_e2e_mysql_db2.tbl2_1 values (7,'db2_tb2_1',2101)",
                "insert into test_e2e_mysql_db2.tbl2_2 values (8,'db2_tb2_2',2102)",
                "insert into test_e2e_mysql_db2.tbl3 values (3,'db2_tb3',220)");

        Thread.sleep(20000);

        List<String> incrExpected =
                Arrays.asList("1,db1_tb1,18", "2,db2_tb1,20", "3,db1_tb1,180", "4,db2_tb1,200");
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, incrExpected, sql1, 3, false);

        List<String> incrExpected2 =
                Arrays.asList(
                        "1,db1_tb2_1,19",
                        "2,db1_tb2_2,191",
                        "3,db2_tb2_2,21",
                        "4,db2_tbl2_2,211",
                        "5,db1_tb2_1,1901",
                        "6,db1_tb2_2,1902",
                        "7,db2_tb2_1,2101",
                        "8,db2_tb2_2,2102");
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, incrExpected2, sql2, 3, false);

        List<String> incrExpected3 = Arrays.asList("1,db2_tb3,22", "3,db2_tb3,220");
        ContainerUtils.checkResult(getDorisQueryConnection(), LOG, incrExpected3, sql3, 3, false);

        cancelE2EJob(jobName);
    }

    @After
    public void close() {
        try {
            // Ensure that semaphore is always released
        } finally {
            LOG.info("Mysql2DorisE2ECase releasing semaphore.");
            SEMAPHORE.release();
        }
    }
}
