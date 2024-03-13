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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

public abstract class DorisTestBase {
    protected static final Logger LOG = LoggerFactory.getLogger(DorisTestBase.class);
    private static final String DEFAULT_DOCKER_IMAGE = "apache/doris:doris-all-in-one-2.1.0";
    protected static final String DORIS_DOCKER_IMAGE =
            System.getProperty("image") == null
                    ? DEFAULT_DOCKER_IMAGE
                    : System.getProperty("image");
    private static final String DRIVER_JAR =
            "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";
    protected static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    protected static final String URL = "jdbc:mysql://%s:9030";
    protected static final String USERNAME = "root";
    protected static final String PASSWORD = "";
    protected static final GenericContainer DORIS_CONTAINER = createDorisContainer();

    protected static String getFenodes() {
        return DORIS_CONTAINER.getHost() + ":8030";
    }

    static {
        startContainers();
    }

    public static void startContainers() {
        try {
            LOG.info("Starting doris containers...");
            // singleton doris container
            DORIS_CONTAINER.start();
            initializeJdbcConnection();
        } catch (Exception ex) {
            LOG.error("Failed to start containers doris, ", ex);
        }
        LOG.info("Containers doris are started.");
    }

    public static GenericContainer createDorisContainer() {
        LOG.info("Create doris containers...");
        GenericContainer container =
                new GenericContainer<>(DORIS_DOCKER_IMAGE)
                        .withNetwork(Network.newNetwork())
                        .withNetworkAliases("DorisContainer")
                        .withPrivilegedMode(true)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(DORIS_DOCKER_IMAGE)))
                        .withExposedPorts(8030, 9030, 8040, 9060);

        container.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", "8030", "8030"),
                        String.format("%s:%s", "9030", "9030"),
                        String.format("%s:%s", "9060", "9060"),
                        String.format("%s:%s", "8040", "8040")));

        return container;
    }

    protected static void initializeJdbcConnection() throws Exception {
        URLClassLoader urlClassLoader =
                new URLClassLoader(
                        new URL[] {new URL(DRIVER_JAR)}, DorisTestBase.class.getClassLoader());
        LOG.info("Try to connect to Doris...");
        Thread.currentThread().setContextClassLoader(urlClassLoader);
        try (Connection connection =
                        DriverManager.getConnection(
                                String.format(URL, DORIS_CONTAINER.getHost()), USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            ResultSet resultSet;
            do {
                LOG.info("Wait for the Backend to start successfully...");
                resultSet = statement.executeQuery("show backends");
            } while (!isBeReady(resultSet, Duration.ofSeconds(1L)));
        }
        LOG.info("Connected to Doris successfully...");
        printClusterStatus();
    }

    private static boolean isBeReady(ResultSet rs, Duration duration) throws SQLException {
        LockSupport.parkNanos(duration.toNanos());
        if (rs.next()) {
            String isAlive = rs.getString("Alive").trim();
            String totalCap = rs.getString("TotalCapacity").trim();
            return "true".equalsIgnoreCase(isAlive) && !"0.000".equalsIgnoreCase(totalCap);
        }
        return false;
    }

    protected static void printClusterStatus() throws Exception {
        LOG.info("Current machine IP: {}", InetAddress.getLocalHost());
        echo("sh", "-c", "cat /proc/cpuinfo | grep 'cpu cores' | uniq");
        echo("sh", "-c", "free -h");
        try (Connection connection =
                        DriverManager.getConnection(
                                String.format(URL, DORIS_CONTAINER.getHost()), USERNAME, PASSWORD);
                Statement statement = connection.createStatement()) {
            ResultSet showFrontends = statement.executeQuery("show frontends");
            LOG.info("Frontends status: {}", convertList(showFrontends));
            ResultSet showBackends = statement.executeQuery("show backends");
            LOG.info("Backends status: {}", convertList(showBackends));
        }
    }

    static void echo(String... cmd) {
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            InputStream is = p.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            p.waitFor();
            is.close();
            reader.close();
            p.destroy();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Map> convertList(ResultSet rs) throws SQLException {
        List<Map> list = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            Map<String, Object> rowData = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                rowData.put(metaData.getColumnName(i), rs.getObject(i));
            }
            list.add(rowData);
        }
        return list;
    }
}
