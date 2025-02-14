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

package org.apache.doris.flink.connection;

import org.apache.doris.flink.cfg.DorisConnectionOptions;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class SimpleJdbcConnectionProviderTest {

    @Test
    public void testGetOrEstablishConnection() throws Exception {
        MockedStatic<DriverManager> driverManagerMockedStatic = mockStatic(DriverManager.class);
        Connection connection = mock(Connection.class);
        when(DriverManager.getConnection(any(), any(), any())).thenReturn(connection);

        DorisConnectionOptions connectionOptions =
                new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                        .withFenodes("127.0.0.1:8030")
                        .withJdbcUrl("jdbc:mysql://127.0.0.1:9030")
                        .withUsername("root")
                        .withPassword("")
                        .build();
        SimpleJdbcConnectionProvider connectionProvider =
                new SimpleJdbcConnectionProvider(connectionOptions);
        connectionProvider.getOrEstablishConnection();

        connectionOptions =
                new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                        .withFenodes("127.0.0.1:8030")
                        .withJdbcUrl("jdbc:mysql://127.0.0.1:9030")
                        .build();
        connectionProvider = new SimpleJdbcConnectionProvider(connectionOptions);
        when(DriverManager.getConnection(any(), any(), any())).thenReturn(connection);
        connectionProvider.getOrEstablishConnection();

        driverManagerMockedStatic.close();
    }
}
