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

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.connection.SimpleJdbcConnectionProvider;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.doris.flink.sink.writer.tvf.TvfSqlUtils.quoteIdentifier;
import static org.apache.doris.flink.sink.writer.tvf.TvfSqlUtils.quoteLiteral;

/** JDBC implementation of TVF load submission and label reconciliation. */
class JdbcS3TvfLoadClient implements S3TvfLoadClient {

    private static final Pattern SESSION_VARIABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final SimpleJdbcConnectionProvider connectionProvider;

    public JdbcS3TvfLoadClient(DorisOptions dorisOptions) {
        this(new SimpleJdbcConnectionProvider(dorisOptions));
    }

    JdbcS3TvfLoadClient(SimpleJdbcConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    @Override
    public void executeInsert(String sql, Map<String, String> sessionVariables)
            throws SQLException {
        try (Statement statement = connection().createStatement()) {
            for (Map.Entry<String, String> entry : sessionVariables.entrySet()) {
                if (!SESSION_VARIABLE.matcher(entry.getKey()).matches()) {
                    throw new SQLException("Invalid Doris session variable: " + entry.getKey());
                }
                statement.execute(
                        "SET SESSION " + entry.getKey() + " = " + quoteLiteral(entry.getValue()));
            }
            statement.execute(sql);
        }
    }

    @Override
    public S3TvfLoadState getLoadState(String database, String label) throws SQLException {
        String sql =
                "SHOW LOAD FROM "
                        + quoteIdentifier(database)
                        + " WHERE LABEL = "
                        + quoteLiteral(label);
        try (Statement statement = connection().createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            S3TvfLoadState resolvedState = S3TvfLoadState.NOT_FOUND;
            // A failed retry can add a CANCELLED row beside the original FINISHED row.
            while (resultSet.next()) {
                S3TvfLoadState state = S3TvfLoadState.UNKNOWN;
                String stateValue = resultSet.getString("State");
                if (stateValue != null) {
                    try {
                        state = S3TvfLoadState.valueOf(stateValue.toUpperCase(Locale.ROOT));
                    } catch (IllegalArgumentException ignored) {
                        // Keep UNKNOWN for unrecognized Doris states.
                    }
                }
                if (state == S3TvfLoadState.FINISHED) {
                    return state;
                }
                if (state.isActive()
                        || (state == S3TvfLoadState.CANCELLED && !resolvedState.isActive())
                        || resolvedState == S3TvfLoadState.NOT_FOUND) {
                    resolvedState = state;
                }
            }
            return resolvedState;
        }
    }

    @Override
    public void cancelLoad(String database, String label) throws SQLException {
        String sql =
                "CANCEL LOAD FROM "
                        + quoteIdentifier(database)
                        + " WHERE LABEL = "
                        + quoteLiteral(label);
        try (Statement statement = connection().createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public void close() throws IOException {
        connectionProvider.closeConnection();
    }

    private Connection connection() throws SQLException {
        try {
            return connectionProvider.getOrEstablishConnection();
        } catch (ClassNotFoundException e) {
            throw new SQLException("Doris JDBC driver is unavailable.", e);
        }
    }
}
