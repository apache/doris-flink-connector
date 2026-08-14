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

package org.apache.doris.flink.catalog;

import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.apache.doris.flink.cfg.DorisConnectionOptions;
import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DorisCatalogTest {

    private DorisCatalog catalog;

    @Before
    public void setup()
            throws DatabaseAlreadyExistException,
                    TableAlreadyExistException,
                    TableNotExistException,
                    DatabaseNotExistException {
        DorisConnectionOptions connectionOptions =
                new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                        .withFenodes("127.0.0.1:8030")
                        .withJdbcUrl("jdbc:mysql://127.0.0.1:8030")
                        .withUsername("root")
                        .withPassword("xxxxx")
                        .build();

        Map<String, String> props = new HashMap<>();
        catalog = new DorisCatalog("catalog_test", connectionOptions, "test", props);
    }

    @Test(expected = Exception.class)
    public void testQueryFenodes() {
        catalog.queryFenodes();
    }

    @Test
    public void testQueryFenodesUsesTlsJdbcProperties() throws Exception {
        String jdbcUrl = "jdbc:mysql://localhost:9030";
        DorisConnectionOptions tlsConnectionOptions =
                new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                        .withFenodes("localhost:8030")
                        .withJdbcUrl(jdbcUrl)
                        .withUsername("root")
                        .withPassword("secret")
                        .withTlsOptions(
                                DorisTlsOptions.builder()
                                        .setEnabled(true)
                                        .setSkipHostnameVerification(true)
                                        .build())
                        .build();
        DorisCatalog tlsCatalog =
                new DorisCatalog("tls_catalog", tlsConnectionOptions, "test", new HashMap<>());
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(connection.prepareStatement("SHOW FRONTENDS")).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnName(1)).thenReturn("Host");
        when(resultSet.next()).thenReturn(false);
        AtomicReference<Properties> capturedProperties = new AtomicReference<>();

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            driverManager
                    .when(() -> DriverManager.getConnection(eq(jdbcUrl), any(Properties.class)))
                    .thenAnswer(
                            invocation -> {
                                capturedProperties.set(invocation.getArgument(1));
                                return connection;
                            });

            Assert.assertEquals("", tlsCatalog.queryFenodes());
        }

        Assert.assertEquals("root", capturedProperties.get().getProperty("user"));
        Assert.assertEquals("secret", capturedProperties.get().getProperty("password"));
        Assert.assertEquals("VERIFY_CA", capturedProperties.get().getProperty("sslMode"));
    }
}
