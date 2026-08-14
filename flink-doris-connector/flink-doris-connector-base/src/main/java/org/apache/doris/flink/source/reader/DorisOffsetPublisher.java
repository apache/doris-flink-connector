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

package org.apache.doris.flink.source.reader;

import org.apache.doris.flink.connection.JdbcConnectionProvider;
import org.apache.doris.flink.exception.DorisRuntimeException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.function.Consumer;

/** Publishes completed-checkpoint offsets through Doris JDBC. */
public class DorisOffsetPublisher implements AutoCloseable {
    private final JdbcConnectionProvider connectionProvider;
    private final String insertSql;
    private final String consumerId;

    public DorisOffsetPublisher(
            JdbcConnectionProvider connectionProvider, String offsetTable, String consumerId) {
        this.connectionProvider = connectionProvider;
        String[] tableParts = offsetTable.split("\\.", -1);
        if (tableParts.length != 2 || tableParts[0].isEmpty() || tableParts[1].isEmpty()) {
            throw new IllegalArgumentException("Offset table must use database.table format");
        }
        this.insertSql =
                "INSERT INTO "
                        + quoteIdentifier(tableParts[0])
                        + "."
                        + quoteIdentifier(tableParts[1])
                        + " (`consumer_id`, `offset_timestamp`, `update_time`) "
                        + "VALUES (?, ?, CURRENT_TIMESTAMP(3))";
        this.consumerId = consumerId;
    }

    /** Invokes the callback with null on success or the publication error on failure. */
    public void publish(String offset, Consumer<Exception> callback) {
        Exception failure = null;
        try {
            Connection connection = connectionProvider.getOrEstablishConnection();
            try (PreparedStatement statement = connection.prepareStatement(insertSql)) {
                statement.setString(1, consumerId);
                statement.setString(2, offset);
                if (statement.executeUpdate() <= 0) {
                    throw new DorisRuntimeException("Doris offset INSERT did not update a row");
                }
            }
        } catch (Exception error) {
            failure = error;
        }
        callback.accept(failure);
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    @Override
    public void close() {
        connectionProvider.closeConnection();
    }
}
