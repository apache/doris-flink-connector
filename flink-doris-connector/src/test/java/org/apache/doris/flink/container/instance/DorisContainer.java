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

package org.apache.doris.flink.container.instance;

import com.github.dockerjava.api.command.RestartContainerCmd;
import com.google.common.collect.Lists;
import org.apache.doris.flink.container.config.DorisPorts.BE;
import org.apache.doris.flink.container.config.DorisPorts.FE;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.shaded.org.awaitility.core.ConditionTimeoutException;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class DorisContainer implements ContainerService {
    private static final Logger LOG = LoggerFactory.getLogger(DorisContainer.class);
    private static final String DEFAULT_DOCKER_IMAGE = "yagagagaga/doris-standalone:3.0.4";
    private static final String DORIS_DOCKER_IMAGE =
            System.getProperty("image") == null
                    ? DEFAULT_DOCKER_IMAGE
                    : System.getProperty("image");
    private static final String DRIVER_JAR =
            "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";
    private static final String JDBC_URL = "jdbc:mysql://%s:9030";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private final GenericContainer<?> dorisContainer;
    private final String systemTimeZone = ZoneId.systemDefault().getId();
    private static URLClassLoader jdbcClassLoader;
    private static final AtomicBoolean driverInitialized = new AtomicBoolean(false);

    public DorisContainer() {
        dorisContainer = createDorisContainer();
    }

    public GenericContainer<?> createDorisContainer() {
        LOG.info("Will create doris containers.");
        GenericContainer<?> container =
                new GenericContainer<>(DORIS_DOCKER_IMAGE)
                        .withNetwork(Network.newNetwork())
                        .withNetworkAliases("DorisContainer")
                        .withPrivilegedMode(true)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(DORIS_DOCKER_IMAGE)))
                        .withCommand(
                                "sh",
                                "-c",
                                "chmod -R 644 /root/be/conf/be.conf /root/fe/conf/fe.conf && chmod -R 755 /root/be/conf /root/fe/conf && chown -R root:root /root/be/conf /root/fe/conf")
                        .withClasspathResourceMapping(
                                "docker/doris/be.conf",
                                "/root/be/conf/be.conf",
                                BindMode.READ_WRITE)
                        .withClasspathResourceMapping(
                                "docker/doris/fe.conf",
                                "/root/fe/conf/fe.conf",
                                BindMode.READ_WRITE)
                        // These exposed ports are used to connect to Doris. They are the default
                        // ports for yagagagaga/doris-standalone:2.1.7.
                        // For more information, see:
                        // https://hub.docker.com/r/yagagagaga/doris-standalone
                        .withExposedPorts(
                                FE.HTTP_PORT,
                                FE.QUERY_PORT,
                                BE.THRIFT_PORT,
                                BE.WEBSERVICE_PORT,
                                FE.FLIGHT_SQL_PORT,
                                BE.FLIGHT_SQL_PORT)
                        .withStartupTimeout(Duration.ofMinutes(5))
                        .withEnv("TZ", systemTimeZone);

        container.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", FE.HTTP_PORT, FE.HTTP_PORT),
                        String.format("%s:%s", FE.QUERY_PORT, FE.QUERY_PORT),
                        String.format("%s:%s", BE.THRIFT_PORT, BE.THRIFT_PORT),
                        String.format("%s:%s", BE.WEBSERVICE_PORT, BE.WEBSERVICE_PORT),
                        String.format("%s:%s", FE.FLIGHT_SQL_PORT, FE.FLIGHT_SQL_PORT),
                        String.format("%s:%s", BE.FLIGHT_SQL_PORT, BE.FLIGHT_SQL_PORT)));
        return container;
    }

    public void startContainer() {
        try {
            if (dorisContainer.isRunning()) {
                return;
            }
            LOG.info("Starting doris containers.");
            // singleton doris container
            dorisContainer.start();
            // print Doris configuration information.
            ExecResult feExecResult =
                    dorisContainer.execInContainer("cat", "/root/fe/conf/fe.conf");
            ExecResult beExecResult =
                    dorisContainer.execInContainer("cat", "/root/be/conf/be.conf");
            LOG.info("FE config: {}", feExecResult.getStdout());
            LOG.info("BE config: {}", beExecResult.getStdout());
            initializeJdbcConnection();
            initializeVariables();
            printClusterStatus();
        } catch (Exception ex) {
            LOG.error("Failed to start containers doris", ex);
            throw new DorisRuntimeException("Failed to start containers doris", ex);
        }
        LOG.info("Doris container started successfully.");
    }

    @Override
    public void restartContainer() {
        LOG.info("Restarting Doris container...");

        try (RestartContainerCmd restartCmd =
                dorisContainer
                        .getDockerClient()
                        .restartContainerCmd(dorisContainer.getContainerId())) {
            restartCmd.exec();
            LOG.info("Restart command executed, waiting for container services to be ready");
            initializeJdbcConnection();
        } catch (Exception e) {
            LOG.error("Failed to restart Doris container", e);
            throw new RuntimeException("Container restart failed", e);
        }

        LOG.info("Doris container successfully restarted and services are ready");
    }

    @Override
    public boolean isRunning() {
        return dorisContainer.isRunning();
    }

    @Override
    public Connection getQueryConnection() {
        LOG.info("Try to get query connection from doris.");
        String jdbcUrl = String.format(JDBC_URL, dorisContainer.getHost());
        try {
            return DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
        } catch (SQLException e) {
            LOG.info("Failed to get doris query connection. jdbcUrl={}", jdbcUrl, e);
            throw new DorisRuntimeException(e);
        }
    }

    private void initializeVariables() throws Exception {
        try (Connection connection = getQueryConnection();
                Statement statement = connection.createStatement()) {
            LOG.info("init doris cluster variables.");
            // avoid arrow flight sql reading bug
            statement.executeQuery("SET PROPERTY FOR 'root' 'max_user_connections' = '1024';");
        }
        LOG.info("Init variables successfully.");
    }

    @Override
    public String getJdbcUrl() {
        return String.format(JDBC_URL, dorisContainer.getHost());
    }

    @Override
    public String getInstanceHost() {
        return dorisContainer.getHost();
    }

    @Override
    public Integer getMappedPort(int originalPort) {
        return dorisContainer.getMappedPort(originalPort);
    }

    @Override
    public String getUsername() {
        return USERNAME;
    }

    @Override
    public String getPassword() {
        return PASSWORD;
    }

    @Override
    public String getFenodes() {
        return dorisContainer.getHost() + ":" + FE.HTTP_PORT;
    }

    @Override
    public String getBenodes() {
        return dorisContainer.getHost() + ":" + BE.WEBSERVICE_PORT;
    }

    public void close() {
        if (dorisContainer != null) {
            LOG.info("Doris container is about to be close.");
            dorisContainer.close();
            LOG.info("Doris container closed successfully.");
        }

        closeJdbcClassLoader();
    }

    private void initializeJDBCDriver() throws MalformedURLException {
        // Checks if the driver has already been initialized to avoid memory leak and class loading
        // issues.
        if (driverInitialized.get()) {
            LOG.debug("JDBC driver already initialized, skipping initialization");
            return;
        }

        LOG.info("Initializing JDBC driver");
        if (jdbcClassLoader == null) {
            jdbcClassLoader =
                    new URLClassLoader(
                            new URL[] {new URL(DRIVER_JAR)}, DorisContainer.class.getClassLoader());
        }

        Thread.currentThread().setContextClassLoader(jdbcClassLoader);
        driverInitialized.set(true);
    }

    private void initializeJdbcConnection() throws Exception {
        initializeJDBCDriver();
        // before connecting to Doris, wait for Doris FE to start,which is to avoid connect Doris
        // failed when Doris FE is not ready.
        waitDorisFeRunning();
        try (Connection connection = getQueryConnection();
                Statement statement = connection.createStatement()) {
            ResultSet resultSet;
            do {
                LOG.info("Waiting for the Backend to start successfully.");
                resultSet = statement.executeQuery("show backends");
            } while (!isBeReady(resultSet, Duration.ofSeconds(1L)));
        }
        LOG.info("Connected to Doris successfully.");
    }

    private synchronized void closeJdbcClassLoader() {
        if (jdbcClassLoader != null) {
            try {
                jdbcClassLoader.close();
                jdbcClassLoader = null;
                driverInitialized.set(false);
                LOG.info("JDBC class loader closed successfully");
            } catch (IOException e) {
                LOG.warn("Failed to close JDBC class loader", e);
            }
        }
    }

    /**
     * Wait for Doris container to running. If the Doris FE not startup completely, try to connect
     * it again until the doris FE is ready.
     */
    private void waitDorisFeRunning() {
        LOG.info("Waiting for Doris services to be accessible...");

        // Poll Doris FE HTTP service every second with a maximum wait time of 5 minutes
        // If the service is not available within this time, a timeout exception will be thrown
        try {
            Awaitility.await("FE HTTP Service")
                    .atMost(5, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                try {
                                    ExecResult result =
                                            dorisContainer.execInContainer(
                                                    "curl",
                                                    "-s",
                                                    "-o",
                                                    "/dev/null",
                                                    "-w",
                                                    "%{http_code}",
                                                    "-m",
                                                    "2",
                                                    "http://localhost:" + FE.HTTP_PORT);
                                    boolean ready = result.getStdout().equals("200");
                                    LOG.info(
                                            "FE HTTP service on port {} is ready: {}",
                                            FE.HTTP_PORT,
                                            ready);

                                    if (ready) {
                                        LOG.info(
                                                "FE HTTP service on port {} is ready",
                                                FE.HTTP_PORT);
                                    }
                                    return ready;
                                } catch (Exception e) {
                                    LOG.debug(
                                            "Exception while checking FE HTTP service: {}",
                                            e.getMessage());
                                    return false;
                                }
                            });

        } catch (ConditionTimeoutException e) {
            LOG.warn("Timed out after 5 minutes waiting for Doris services to be ready");
        }
    }

    private boolean isBeReady(ResultSet rs, Duration duration) throws SQLException {
        LockSupport.parkNanos(duration.toNanos());
        if (rs.next()) {
            String isAlive = rs.getString("Alive").trim();
            String totalCap = rs.getString("TotalCapacity").trim();
            return Boolean.toString(true).equalsIgnoreCase(isAlive)
                    && !"0.000".equalsIgnoreCase(totalCap);
        }
        return false;
    }

    private void printClusterStatus() throws Exception {
        LOG.info("Current machine IP: {}", dorisContainer.getHost());
        echo("sh", "-c", "cat /proc/cpuinfo | grep 'cpu cores' | uniq");
        echo("sh", "-c", "free -h");
        try (Connection connection =
                        DriverManager.getConnection(
                                String.format(JDBC_URL, dorisContainer.getHost()),
                                USERNAME,
                                PASSWORD);
                Statement statement = connection.createStatement()) {
            ResultSet showFrontends = statement.executeQuery("show frontends");
            LOG.info("Frontends status: {}", convertList(showFrontends));
            ResultSet showBackends = statement.executeQuery("show backends");
            LOG.info("Backends status: {}", convertList(showBackends));
        }
    }

    private void echo(String... cmd) {
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
            LOG.info("Execute command in docker container failed. ", e);
        }
    }

    private List<Map<String, Object>> convertList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> list = new ArrayList<>();
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
