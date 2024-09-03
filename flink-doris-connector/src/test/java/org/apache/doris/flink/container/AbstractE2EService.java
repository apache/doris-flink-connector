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

package org.apache.doris.flink.container;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.container.instance.ContainerService;
import org.apache.doris.flink.container.instance.MySQLContainer;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.tools.cdc.CdcTools;
import org.apache.doris.flink.tools.cdc.DatabaseSyncConfig;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;

public abstract class AbstractE2EService extends AbstractContainerTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractE2EService.class);
    private static ContainerService mysqlContainerService;
    private static JobClient jobClient;
    protected static final Semaphore SEMAPHORE = new Semaphore(1);
    protected static final String SINK_CONF = "--" + DatabaseSyncConfig.SINK_CONF;
    protected static final String DORIS_DATABASE = "--database";
    protected static final String HOSTNAME = "hostname";
    protected static final String PORT = "port";
    protected static final String USERNAME = "username";
    protected static final String PASSWORD = "password";
    protected static final String DATABASE_NAME = "database-name";
    protected static final String FENODES = "fenodes";
    protected static final String JDBC_URL = "jdbc-url";
    protected static final String SINK_LABEL_PREFIX = "sink.label-prefix";

    @BeforeClass
    public static void initE2EContainers() {
        LOG.info("Trying to Start init E2E containers.");
        initMySQLContainer();
    }

    private static void initMySQLContainer() {
        if (Objects.nonNull(mysqlContainerService) && mysqlContainerService.isRunning()) {
            LOG.info("The MySQL container has been started and is running status.");
            return;
        }
        mysqlContainerService = new MySQLContainer();
        mysqlContainerService.startContainer();
        LOG.info("Mysql container was started.");
    }

    protected String getMySQLInstanceHost() {
        return mysqlContainerService.getInstanceHost();
    }

    protected Integer getMySQLQueryPort() {
        return mysqlContainerService.getMappedPort(3306);
    }

    protected String getMySQLUsername() {
        return mysqlContainerService.getUsername();
    }

    protected String getMySQLPassword() {
        return mysqlContainerService.getPassword();
    }

    protected Connection getMySQLQueryConnection() {
        return mysqlContainerService.getQueryConnection();
    }

    protected void submitE2EJob(String jobName, String[] args) {
        try {
            LOG.info("{} e2e job will submit to start. ", jobName);
            CdcTools.setStreamExecutionEnvironmentForTesting(configFlinkEnvironment());
            CdcTools.main(args);
            jobClient = CdcTools.getJobClient();
            if (Objects.isNull(jobClient)) {
                LOG.warn("Failed get flink job client. jobName={}", jobName);
                throw new DorisRuntimeException("Failed get flink job client. jobName=" + jobName);
            }
        } catch (Exception e) {
            LOG.warn("Failed to submit e2e job. jobName={}", jobName);
            throw new DorisRuntimeException(e);
        }
    }

    protected void cancelE2EJob(String jobName) {
        LOG.info("{} e2e job will cancel", jobName);
        jobClient.cancel();
    }

    private StreamExecutionEnvironment configFlinkEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, String> flinkMap = new HashMap<>();
        flinkMap.put("execution.checkpointing.interval", "10s");
        flinkMap.put("pipeline.operator-chaining", "false");
        flinkMap.put("parallelism.default", "1");
        Configuration configuration = Configuration.fromMap(flinkMap);
        env.configure(configuration);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return env;
    }

    protected void setSinkConfDefaultConfig(List<String> argList) {
        // set default doris sink config
        argList.add(SINK_CONF);
        argList.add(FENODES + "=" + getFenodes());
        argList.add(SINK_CONF);
        argList.add(USERNAME + "=" + getDorisUsername());
        argList.add(SINK_CONF);
        argList.add(PASSWORD + "=" + getDorisPassword());
        argList.add(SINK_CONF);
        argList.add(FENODES + "=" + getFenodes());
        argList.add(SINK_CONF);
        argList.add(JDBC_URL + "=" + getDorisQueryUrl());
        argList.add(SINK_CONF);
        argList.add(SINK_LABEL_PREFIX + "=" + "label");
    }

    public static void closeE2EContainers() {
        LOG.info("Starting to close E2E containers.");
        closeMySQLContainer();
    }

    private static void closeMySQLContainer() {
        if (Objects.isNull(mysqlContainerService)) {
            return;
        }
        mysqlContainerService.close();
        LOG.info("Mysql container was closed.");
    }
}
