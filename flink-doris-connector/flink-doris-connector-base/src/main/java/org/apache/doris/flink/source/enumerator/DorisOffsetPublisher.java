// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.doris.flink.source.enumerator;

import org.apache.doris.flink.connection.JdbcConnectionProvider;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.source.split.DorisStreamSplit;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Optional;

/** Reads startup offsets and publishes completed-checkpoint offsets through Doris JDBC. */
public class DorisOffsetPublisher implements AutoCloseable {
    private final JdbcConnectionProvider connectionProvider;
    private final String database;
    private final String table;
    private final String insertSql;
    private final String consumerId;

    public DorisOffsetPublisher(
            JdbcConnectionProvider connectionProvider, String offsetTable, String consumerId) {
        this.connectionProvider = connectionProvider;
        String[] tableParts = offsetTable.split("\\.", -1);
        if (tableParts.length != 2 || tableParts[0].isEmpty() || tableParts[1].isEmpty()) {
            throw new IllegalArgumentException("Offset table must use database.table format");
        }
        this.database = tableParts[0];
        this.table = tableParts[1];
        this.insertSql =
                "INSERT INTO "
                        + quoteIdentifier(database)
                        + "."
                        + quoteIdentifier(table)
                        + " (`consumer_id`, `offset_timestamp`, `update_time`) "
                        + "VALUES (?, ?, CURRENT_TIMESTAMP(3))";
        this.consumerId = consumerId;
    }

    public void validateOffsetTable() {
        try {
            Connection connection = connectionProvider.getOrEstablishConnection();
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tables = metadata.getTables(database, null, table, null)) {
                while (tables.next()) {
                    if (table.equals(tables.getString("TABLE_NAME"))) {
                        return;
                    }
                }
            }
        } catch (Exception error) {
            throw new DorisRuntimeException(
                    "Failed to validate offset table: " + database + "." + table, error);
        }
        throw new DorisRuntimeException("Offset table does not exist: " + database + "." + table);
    }

    /** Returns empty only when this consumer has no saved offset. */
    public Optional<String> readOffset() {
        String selectSql =
                "SELECT `offset_timestamp` FROM "
                        + quoteIdentifier(database)
                        + "."
                        + quoteIdentifier(table)
                        + " WHERE `consumer_id` = ?";
        try (PreparedStatement statement =
                connectionProvider.getOrEstablishConnection().prepareStatement(selectSql)) {
            statement.setString(1, consumerId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                String offset = result.getString("offset_timestamp");
                if (result.next()) {
                    throw new DorisRuntimeException("Multiple offsets for consumer " + consumerId);
                }
                String invalidOffset =
                        "Invalid offset timestamp for consumer " + consumerId + ": " + offset;
                if (!DorisStreamSplit.isValidTimestamp(offset)) {
                    throw new DorisRuntimeException(invalidOffset);
                }
                try {
                    LocalDateTime.parse(
                            offset,
                            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
                                    .withResolverStyle(ResolverStyle.STRICT));
                } catch (DateTimeParseException error) {
                    throw new DorisRuntimeException(invalidOffset, error);
                }
                return Optional.of(offset);
            }
        } catch (DorisRuntimeException error) {
            throw error;
        } catch (Exception error) {
            throw new DorisRuntimeException(
                    "Failed to read offset from "
                            + database
                            + "."
                            + table
                            + " for consumer "
                            + consumerId,
                    error);
        }
    }

    /** Writes a completed-checkpoint offset; the caller owns asynchronous execution and retries. */
    public void publish(String offset) throws Exception {
        Connection connection = connectionProvider.getOrEstablishConnection();
        try (PreparedStatement statement = connection.prepareStatement(insertSql)) {
            statement.setString(1, consumerId);
            statement.setString(2, offset);
            if (statement.executeUpdate() <= 0) {
                throw new DorisRuntimeException("Doris offset INSERT did not update a row");
            }
        }
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    @Override
    public void close() {
        connectionProvider.closeConnection();
    }
}
