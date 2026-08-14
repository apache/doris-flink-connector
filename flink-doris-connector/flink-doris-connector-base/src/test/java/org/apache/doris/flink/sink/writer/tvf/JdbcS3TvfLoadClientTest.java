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

package org.apache.doris.flink.sink.writer.tvf;

import org.apache.doris.flink.connection.SimpleJdbcConnectionProvider;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcS3TvfLoadClientTest {

    @Test
    public void testReturnsFinishedWhenRetryCreatesCancelledLoad() throws Exception {
        SimpleJdbcConnectionProvider connectionProvider = mock(SimpleJdbcConnectionProvider.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connectionProvider.getOrEstablishConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(org.mockito.ArgumentMatchers.anyString()))
                .thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("State")).thenReturn("CANCELLED", "FINISHED");

        S3TvfLoadState state =
                new JdbcS3TvfLoadClient(connectionProvider).getLoadState("db", "label");

        Assert.assertEquals(S3TvfLoadState.FINISHED, state);
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(statement).executeQuery(sql.capture());
        Assert.assertFalse(sql.getValue().contains("LIMIT"));
    }
}
