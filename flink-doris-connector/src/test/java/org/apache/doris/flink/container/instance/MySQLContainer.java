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

import org.apache.doris.flink.exception.DorisRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Stream;

public class MySQLContainer implements ContainerService {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLContainer.class);
    private static final String MYSQL_VERSION = "mysql:8.0";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "123456";
    private final org.testcontainers.containers.MySQLContainer mysqlcontainer;

    public MySQLContainer() {
        mysqlcontainer = createContainer();
    }

    private org.testcontainers.containers.MySQLContainer createContainer() {
        LOG.info("Will create mysql container.");
        return new org.testcontainers.containers.MySQLContainer(MYSQL_VERSION)
                .withUsername(USERNAME)
                .withPassword(PASSWORD);
    }

    @Override
    public void startContainer() {
        LOG.info("Starting MySQL container.");
        Startables.deepStart(Stream.of(mysqlcontainer)).join();
        LOG.info("MySQL Container was started.");
    }

    @Override
    public boolean isRunning() {
        return mysqlcontainer.isRunning();
    }

    @Override
    public String getInstanceHost() {
        return mysqlcontainer.getHost();
    }

    @Override
    public Integer getMappedPort(int originalPort) {
        return mysqlcontainer.getMappedPort(originalPort);
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
    public Connection getQueryConnection() {
        LOG.info("Try to get query connection from mysql.");
        try {
            return DriverManager.getConnection(mysqlcontainer.getJdbcUrl(), USERNAME, PASSWORD);
        } catch (SQLException e) {
            LOG.warn(
                    "Failed to get mysql container query connection. jdbcUrl={}, user={}",
                    mysqlcontainer.getJdbcUrl(),
                    USERNAME,
                    e);
            throw new DorisRuntimeException(e);
        }
    }

    @Override
    public void close() {
        LOG.info("Stopping MySQL container.");
        mysqlcontainer.stop();
        LOG.info("MySQL Container was stopped.");
    }
}
