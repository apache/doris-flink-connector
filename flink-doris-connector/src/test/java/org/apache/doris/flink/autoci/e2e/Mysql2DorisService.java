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

package org.apache.doris.flink.autoci.e2e;

import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.tools.cdc.DatabaseSyncConfig;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class Mysql2DorisService extends AbstractE2EService {
    private static final Logger LOG = LoggerFactory.getLogger(Mysql2DorisService.class);
    private static final String DATABASE = "test_e2e_mysql";
    private static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS " + DATABASE;
    private static final String MYSQL_CONF = "--" + DatabaseSyncConfig.MYSQL_CONF;

    private void initialize() {
        // init mysql table
        LOG.info("start to init mysql table.");
        E2EContainerUtils.executeSQLStatement(getMySQLQueryConnection(), LOG, CREATE_DATABASE);
        E2EContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                E2EContainerUtils.parseFileContentSQL(
                        "autoci/e2e/mysql2doris/initialize/mysql_tbl1.sql"));
        E2EContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                E2EContainerUtils.parseFileContentSQL(
                        "autoci/e2e/mysql2doris/initialize/mysql_tbl2.sql"));
        E2EContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                E2EContainerUtils.parseFileContentSQL(
                        "autoci/e2e/mysql2doris/initialize/mysql_tbl3.sql"));
        E2EContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                E2EContainerUtils.parseFileContentSQL(
                        "autoci/e2e/mysql2doris/initialize/mysql_tbl4.sql"));
        E2EContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                E2EContainerUtils.parseFileContentSQL(
                        "autoci/e2e/mysql2doris/initialize/mysql_tbl5.sql"));

        // init doris table
        LOG.info("start to init doris table.");
        E2EContainerUtils.executeSQLStatement(getDorisQueryConnection(), LOG, CREATE_DATABASE);
        E2EContainerUtils.executeSQLStatement(
                getDorisQueryConnection(),
                LOG,
                "DROP TABLE IF EXISTS test_e2e_mysql.tbl1",
                "DROP TABLE IF EXISTS test_e2e_mysql.tbl2",
                "DROP TABLE IF EXISTS test_e2e_mysql.tbl3",
                "DROP TABLE IF EXISTS test_e2e_mysql.tbl4",
                "DROP TABLE IF EXISTS test_e2e_mysql.tbl5");
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
        argList.add(DATABASE_NAME + "=" + DATABASE);

        // set doris database
        argList.add(DORIS_DATABASE);
        argList.add(DATABASE);
        setSinkConfDefaultConfig(argList);
        return argList;
    }

    private void startMysql2DorisJob(String jobName, String resourcePath) {
        LOG.info("start a mysql to doris job. jobName={}, resourcePath={}", jobName, resourcePath);
        List<String> argList = E2EContainerUtils.parseFileArgs(resourcePath);
        String[] args = setMysql2DorisDefaultConfig(argList).toArray(new String[0]);
        submitE2EJob(jobName, args);
        verifyInitializeResult();
    }

    private void verifyInitializeResult() {
        // wait 2 times checkpoint
        try {
            Thread.sleep(20000);
            LOG.info("Start to verify init result.");
            List<String> expected =
                    Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3", "doris_5,5");
            String sql1 =
                    "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 union all select * from test_e2e_mysql.tbl5) res order by 1";
            E2EContainerUtils.checkResult(getDorisQueryConnection(), expected, sql1, 2);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMySQL2Doris() throws Exception {
        initialize();
        String jobName = "testMySQL2Doris";
        String resourcePath = "autoci/e2e/mysql2doris/testMySQL2Doris.txt";
        startMysql2DorisJob(jobName, resourcePath);

        addIncrementalData();
        verifyIncrementalDataResult();

        tbl1SchemaChange();
        verifyTbl1SchemaChange();
        cancelCurrentE2EJob(jobName);
    }

    @Test
    public void testAutoAddTable() throws InterruptedException {
        initialize();
        String jobName = "testAutoAddTable";
        startMysql2DorisJob(jobName, "autoci/e2e/mysql2doris/testAutoAddTable.txt");

        // auto add table
        LOG.info("starting to create auto_add table.");
        E2EContainerUtils.executeSQLStatement(
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
        E2EContainerUtils.checkResult(getDorisQueryConnection(), autoAddResult, autoAddSql, 2);

        // incremental data
        LOG.info("starting to increment data.");
        addIncrementalData();
        E2EContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
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
        E2EContainerUtils.checkResult(
                getDorisQueryConnection(), incrementDataExpected, incrementDataSql, 2);

        // schema change
        LOG.info("starting to mock schema change.");
        E2EContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
                "alter table test_e2e_mysql.auto_add add column c1 varchar(128)",
                "alter table test_e2e_mysql.auto_add drop column age",
                "insert into test_e2e_mysql.auto_add values ('doris_4_4','c1_val')");
        Thread.sleep(20000);
        List<String> schemaChangeExpected =
                Arrays.asList("doris_4_1,null", "doris_4_3,null", "doris_4_4,c1_val");
        String schemaChangeSql = "select * from test_e2e_mysql.auto_add order by 1";
        E2EContainerUtils.checkResult(
                getDorisQueryConnection(), schemaChangeExpected, schemaChangeSql, 2);
        cancelCurrentE2EJob(jobName);
    }

    @Test
    public void testMySQL2DorisSQLParse() throws Exception {
        initialize();
        String jobName = "testMySQL2DorisSQLParse";
        String resourcePath = "autoci/e2e/mysql2doris/testMySQL2DorisSQLParse.txt";
        startMysql2DorisJob(jobName, resourcePath);

        addIncrementalData();
        verifyIncrementalDataResult();

        tbl1SchemaChange();
        verifyTbl1SchemaChange();

        // mock create table
        LOG.info("start to create table in mysql.");
        E2EContainerUtils.executeSQLStatement(
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
        E2EContainerUtils.checkResult(
                getDorisQueryConnection(), createTableExpected, createTableSql, 2);
        cancelCurrentE2EJob(jobName);
    }

    private void tbl1SchemaChange() {
        // mock schema change
        LOG.info("start to schema change in mysql.");
        try {
            E2EContainerUtils.executeSQLStatement(
                    getMySQLQueryConnection(),
                    LOG,
                    "alter table test_e2e_mysql.tbl1 add column c1 varchar(128)",
                    "alter table test_e2e_mysql.tbl1 drop column age");
            Thread.sleep(20000);
            E2EContainerUtils.executeSQLStatement(
                    getMySQLQueryConnection(),
                    LOG,
                    "insert into test_e2e_mysql.tbl1  values ('doris_1_1_1','c1_val')");
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            throw new DorisRuntimeException(e);
        }
    }

    private void verifyTbl1SchemaChange() {
        LOG.info("verify tal1 schema change.");
        List<String> schemaChangeExpected =
                Arrays.asList("doris_1,null", "doris_1_1,null", "doris_1_1_1,c1_val");
        String schemaChangeSql = "select * from test_e2e_mysql.tbl1 order by 1";
        E2EContainerUtils.checkResult(
                getDorisQueryConnection(), schemaChangeExpected, schemaChangeSql, 2);
    }

    private void addIncrementalData() {
        // add incremental data
        try {
            E2EContainerUtils.executeSQLStatement(
                    getMySQLQueryConnection(),
                    LOG,
                    "insert into test_e2e_mysql.tbl1 values ('doris_1_1',10)",
                    "insert into test_e2e_mysql.tbl2 values ('doris_2_1',11)",
                    "insert into test_e2e_mysql.tbl3 values ('doris_3_1',12)",
                    "update test_e2e_mysql.tbl1 set age=18 where name='doris_1'",
                    "delete from test_e2e_mysql.tbl2 where name='doris_2'");
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            throw new DorisRuntimeException(e);
        }
    }

    private void verifyIncrementalDataResult() {
        LOG.info("Start to verify incremental data result.");
        List<String> expected2 =
                Arrays.asList(
                        "doris_1,18", "doris_1_1,10", "doris_2_1,11", "doris_3,3", "doris_3_1,12");
        String sql2 =
                "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 ) res order by 1";
        E2EContainerUtils.checkResult(getDorisQueryConnection(), expected2, sql2, 2);
    }

    @Test
    public void testMySQL2DorisByDefault() throws InterruptedException {
        initialize();
        String jobName = "testMySQL2DorisByDefault";
        startMysql2DorisJob(jobName, "autoci/e2e/mysql2doris/testMySQL2DorisByDefault.txt");

        addIncrementalData();
        verifyIncrementalDataResult();
        cancelCurrentE2EJob(jobName);
    }

    @Test
    public void testMySQL2DorisEnableDelete() throws Exception {
        initialize();
        String jobName = "testMySQL2DorisEnableDelete";
        startMysql2DorisJob(jobName, "autoci/e2e/mysql2doris/testMySQL2DorisEnableDelete.txt");

        addIncrementalData();
        E2EContainerUtils.executeSQLStatement(
                getMySQLQueryConnection(),
                LOG,
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
        E2EContainerUtils.checkResult(getDorisQueryConnection(), expected, sql, 2);
        cancelCurrentE2EJob(jobName);
    }
}
