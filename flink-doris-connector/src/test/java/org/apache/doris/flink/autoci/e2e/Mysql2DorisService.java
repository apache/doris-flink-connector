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

import org.apache.doris.flink.tools.cdc.DatabaseSyncConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Mysql2DorisService extends AbstractE2EService {
    private static final Logger LOG = LoggerFactory.getLogger(Mysql2DorisService.class);
    private static final String DATABASE = "test_e2e_mysql";
    private static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS " + DATABASE;
    private static final String MYSQL_CONF = "--" + DatabaseSyncConfig.MYSQL_CONF;
    private static final String SINK_CONF = "--" + DatabaseSyncConfig.SINK_CONF;
    private static final String DORIS_DATABASE = "--database";
    private static final String HOSTNAME = "hostname";
    private static final String PORT = "port";
    private static final String USERNAME = "port";
    private static final String PASSWORD = "password";
    private static final String MYSQL_DATABASE_NAME = "database-name";
    private static final String FENODES = "fenodes";
    private static final String JDBC_URL = "jdbc-url";
    private static final String SINK_LABEL_PREFIX = "sink.label-prefix";

    @Before
    public void setup() {
        // init mysql table
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

    private String[] setMysql2DorisDefaultConfig(String[] args) {
        // set default mysql config
        Set<String> argList = Arrays.stream(args).collect(Collectors.toSet());
        argList.add(MYSQL_CONF + " " + HOSTNAME + "=" + getMySQLInstanceHost());
        argList.add(MYSQL_CONF + " " + PORT + "=" + getMySQLQueryPort());
        argList.add(MYSQL_CONF + " " + USERNAME + "=" + getMySQLUsername());
        argList.add(MYSQL_CONF + " " + PASSWORD + "=" + getMySQLPassword());
        argList.add(MYSQL_CONF + " " + MYSQL_DATABASE_NAME + "=" + DATABASE);

        // set default doris sink config
        argList.add(DORIS_DATABASE + " " + DATABASE);
        argList.add(SINK_CONF + " " + FENODES + "=" + getFenodes());
        argList.add(SINK_CONF + " " + USERNAME + "=" + getDorisUsername());
        argList.add(SINK_CONF + " " + PASSWORD + "=" + getDorisPassword());
        argList.add(SINK_CONF + " " + FENODES + "=" + getFenodes());
        argList.add(SINK_CONF + " " + JDBC_URL + "=" + getDorisQueryUrl());
        argList.add(SINK_CONF + " " + SINK_LABEL_PREFIX + "=" + "label");
        return argList.toArray(new String[0]);
    }

    @Test
    public void testMySQL2DorisByDefault() throws InterruptedException {
        String fileContent =
                E2EContainerUtils.loadFileContent(
                        "autoci/e2e/mysql2doris/testMySQL2DorisByDefault.txt");
        String[] args = setMysql2DorisDefaultConfig(fileContent.split("\n"));
        submitE2EJob(args);

        // wait 2 times checkpoint
        Thread.sleep(20000);
        List<String> expected = Arrays.asList("doris_1,1", "doris_2,2", "doris_3,3", "doris_5,5");
        String sql1 =
                "select * from ( select * from test_e2e_mysql.tbl1 union all select * from test_e2e_mysql.tbl2 union all select * from test_e2e_mysql.tbl3 union all select * from test_e2e_mysql.tbl5) res order by 1";
        E2EContainerUtils.checkResult(getDorisQueryConnection(), expected, sql1, 2);
    }

    @After
    public void close() {}
}
