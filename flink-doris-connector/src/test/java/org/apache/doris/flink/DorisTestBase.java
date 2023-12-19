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

package org.apache.doris.flink;

import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

public abstract class DorisTestBase {
    protected static final Logger LOG = LoggerFactory.getLogger(DorisTestBase.class);
    // protected static final String DORIS_12_DOCKER_IMAGE = "adamlee489/doris:1.2.7.1_arm";
    protected static final String DORIS_12_DOCKER_IMAGE = "adamlee489/doris:1.2.7.1_x86";
    private static final String DRIVER_JAR =
            "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";
    protected static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    protected static final String URL = "jdbc:mysql://%s:9030";
    protected static final String USERNAME = "root";
    protected static final String PASSWORD = "";
    protected static final GenericContainer DORIS_CONTAINER = createDorisContainer();
    protected static Connection connection;
    protected static final int DEFAULT_PARALLELISM = 4;

    protected static String getFenodes() {
        return DORIS_CONTAINER.getHost() + ":8030";
    }

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(DORIS_CONTAINER)).join();
        given().ignoreExceptions()
                .await()
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(DorisTestBase::initializeJdbcConnection);
        LOG.info("Containers are started.");
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        DORIS_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    public static GenericContainer createDorisContainer() {
        GenericContainer container =
                new GenericContainer<>(DORIS_12_DOCKER_IMAGE)
                        .withNetwork(Network.newNetwork())
                        .withNetworkAliases("DorisContainer")
                        .withEnv("FE_SERVERS", "fe1:127.0.0.1:9010")
                        .withEnv("FE_ID", "1")
                        .withEnv("CURRENT_BE_IP", "127.0.0.1")
                        .withEnv("CURRENT_BE_PORT", "9050")
                        .withCommand("ulimit -n 65536")
                        .withCreateContainerCmdModifier(
                                cmd -> cmd.getHostConfig().withMemorySwap(0L))
                        .withPrivilegedMode(true)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(DORIS_12_DOCKER_IMAGE)));

        container.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", "8030", "8030"),
                        String.format("%s:%s", "9030", "9030"),
                        String.format("%s:%s", "9060", "9060"),
                        String.format("%s:%s", "8040", "8040")));

        return container;
    }

    protected static void initializeJdbcConnection() throws SQLException, MalformedURLException {
        URLClassLoader urlClassLoader =
                new URLClassLoader(
                        new URL[] {new URL(DRIVER_JAR)}, DorisTestBase.class.getClassLoader());
        LOG.info("Try to connect to Doris...");
        Thread.currentThread().setContextClassLoader(urlClassLoader);
        connection =
                DriverManager.getConnection(
                        String.format(URL, DORIS_CONTAINER.getHost()), USERNAME, PASSWORD);
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet;
            do {
                LOG.info("Wait for the Backend to start successfully...");
                resultSet = statement.executeQuery("show backends");
            } while (!isBeReady(resultSet, Duration.ofSeconds(1L)));
        }
        LOG.info("Connected to Doris successfully...");
    }

    private static boolean isBeReady(ResultSet rs, Duration duration) throws SQLException {
        if (rs.next()) {
            String isAlive = rs.getString(10).trim();
            String totalCap = rs.getString(16).trim();
            LockSupport.parkNanos(duration.toNanos());
            return "true".equalsIgnoreCase(isAlive) && !"0.000".equalsIgnoreCase(totalCap);
        }
        return false;
    }
}
