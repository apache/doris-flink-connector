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

package org.apache.doris.flink.sink.overwrite;

import org.apache.flink.util.Preconditions;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.connection.SimpleJdbcConnectionProvider;
import org.apache.doris.flink.exception.DorisSystemException;
import org.apache.doris.flink.sink.writer.WriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

/** DDL helper for INSERT OVERWRITE staging writes. */
public class DorisOverwriteManager {
    private static final Logger LOG = LoggerFactory.getLogger(DorisOverwriteManager.class);
    private static final int MAX_STAGING_TABLE_LENGTH = 63;
    private static final String STAGING_PREFIX = "__doris_flink_overwrite_";

    private DorisOverwriteManager() {}

    public static DorisPreparedOverwrite prepareOverwrite(
            DorisOptions targetOptions, DorisExecutionOptions executionOptions) {
        validateOverwriteOptions(targetOptions, executionOptions);
        DorisTableIdentifier targetTable =
                DorisTableIdentifier.of(targetOptions.getTableIdentifier());
        DorisTableIdentifier stagingTable =
                new DorisTableIdentifier(
                        targetTable.getDatabase(),
                        buildStagingTableName(targetTable, executionOptions.getLabelPrefix()));
        DorisOptions stagingOptions = copyOptions(targetOptions, stagingTable.asString());

        Long targetTableId;
        Long stagingTableId;
        SimpleJdbcConnectionProvider jdbcConnectionProvider =
                new SimpleJdbcConnectionProvider(targetOptions);
        try (Connection connection = jdbcConnectionProvider.getOrEstablishConnection()) {
            targetTableId = requireTableId(connection, targetTable);
            if (!tableExists(connection, stagingTable)) {
                execute(connection, createTableLikeSql(stagingTable, targetTable));
            } else {
                throw new DorisSystemException(
                        String.format(
                                "Doris overwrite staging table %s already exists. "
                                        + "Use a unique sink.label-prefix for each INSERT OVERWRITE job "
                                        + "or clean up the leftover staging table after verifying it is unused.",
                                stagingTable));
            }
            stagingTableId = requireTableId(connection, stagingTable);
        } catch (Exception e) {
            throw new DorisSystemException(
                    String.format(
                            "Failed to prepare INSERT OVERWRITE staging table %s for target %s",
                            stagingTable, targetTable),
                    e);
        }

        DorisOverwriteOptions overwriteOptions =
                new DorisOverwriteOptions(
                        targetOptions,
                        targetTable,
                        stagingTable,
                        targetTableId,
                        stagingTableId,
                        executionOptions.getLabelPrefix());
        return new DorisPreparedOverwrite(stagingOptions, overwriteOptions);
    }

    public static void finalizeOverwrite(DorisOverwriteOptions overwriteOptions) {
        DorisOptions targetOptions = overwriteOptions.getTargetOptions();
        SimpleJdbcConnectionProvider jdbcConnectionProvider =
                new SimpleJdbcConnectionProvider(targetOptions);
        try (Connection connection = jdbcConnectionProvider.getOrEstablishConnection()) {
            if (isAlreadyFinalized(connection, overwriteOptions)) {
                LOG.info("Doris overwrite {} has already been finalized.", overwriteOptions);
                return;
            }
            validateTargetUnchanged(connection, overwriteOptions);
            execute(connection, replaceTableSql(overwriteOptions));
        } catch (Exception e) {
            if (isFinalizedAfterFailure(jdbcConnectionProvider, overwriteOptions, e)) {
                LOG.info(
                        "Doris overwrite {} was finalized before the failure returned.",
                        overwriteOptions);
                return;
            }
            throw new DorisSystemException(
                    String.format("Failed to finalize Doris INSERT OVERWRITE %s", overwriteOptions),
                    e);
        }
    }

    public static DorisOptions copyOptions(DorisOptions options, String tableIdentifier) {
        return DorisOptions.builder()
                .setFenodes(options.getFenodes())
                .setBenodes(options.getBenodes())
                .setUsername(options.getUsername())
                .setPassword(options.getPassword())
                .setJdbcUrl(options.getJdbcUrl())
                .setAutoRedirect(options.isAutoRedirect())
                .setTableIdentifier(tableIdentifier)
                .build();
    }

    public static String createTableLikeSql(
            DorisTableIdentifier stagingTable, DorisTableIdentifier targetTable) {
        return "CREATE TABLE " + stagingTable.toSql() + " LIKE " + targetTable.toSql();
    }

    public static String replaceTableSql(DorisOverwriteOptions overwriteOptions) {
        return "ALTER TABLE "
                + overwriteOptions.getTargetTable().toSql()
                + " REPLACE WITH TABLE "
                + DorisTableIdentifier.quote(overwriteOptions.getStagingTable().getTable())
                + " PROPERTIES('swap'='false')";
    }

    public static String buildStagingTableName(DorisTableIdentifier targetTable, String attemptId) {
        String digest =
                DigestUtils.sha256Hex(targetTable.asString() + "|" + attemptId).substring(0, 16);
        String safeAttempt = attemptId.replaceAll("[^A-Za-z0-9_]", "_").toLowerCase(Locale.ROOT);
        if (safeAttempt.isEmpty()) {
            safeAttempt = "job";
        }
        int maxAttemptLength =
                MAX_STAGING_TABLE_LENGTH - STAGING_PREFIX.length() - digest.length() - 1;
        if (safeAttempt.length() > maxAttemptLength) {
            safeAttempt = safeAttempt.substring(0, maxAttemptLength);
        }
        return STAGING_PREFIX + safeAttempt + "_" + digest;
    }

    static void validateOverwriteOptions(
            DorisOptions targetOptions, DorisExecutionOptions executionOptions) {
        Preconditions.checkArgument(
                StringUtils.isNotBlank(targetOptions.getJdbcUrl()),
                "jdbc-url is required for INSERT OVERWRITE staging mode.");
        Preconditions.checkArgument(
                WriteMode.STREAM_LOAD.equals(executionOptions.getWriteMode()),
                "INSERT OVERWRITE staging mode only supports STREAM_LOAD write mode.");
        Preconditions.checkArgument(
                executionOptions.enabled2PC(),
                "INSERT OVERWRITE staging mode requires sink.enable-2pc=true.");
        Preconditions.checkArgument(
                StringUtils.isNotBlank(executionOptions.getLabelPrefix()),
                "sink.label-prefix is required for INSERT OVERWRITE staging mode.");
        Preconditions.checkArgument(
                !executionOptions.ignoreCommitError(),
                "INSERT OVERWRITE staging mode does not support sink.ignore.commit-error=true.");
    }

    private static void validateTargetUnchanged(
            Connection connection, DorisOverwriteOptions overwriteOptions) throws Exception {
        Long expectedTargetId = overwriteOptions.getTargetTableId();
        Long currentTargetId = requireTableId(connection, overwriteOptions.getTargetTable());
        if (!expectedTargetId.equals(currentTargetId)) {
            throw new DorisSystemException(
                    String.format(
                            "Target table %s changed from id %s to id %s before overwrite finalization.",
                            overwriteOptions.getTargetTable(), expectedTargetId, currentTargetId));
        }
    }

    private static boolean isFinalizedAfterFailure(
            SimpleJdbcConnectionProvider jdbcConnectionProvider,
            DorisOverwriteOptions overwriteOptions,
            Exception originalException) {
        try (Connection connection = jdbcConnectionProvider.getOrEstablishConnection()) {
            return isAlreadyFinalized(connection, overwriteOptions);
        } catch (Exception checkException) {
            originalException.addSuppressed(checkException);
            return false;
        }
    }

    private static boolean isAlreadyFinalized(
            Connection connection, DorisOverwriteOptions overwriteOptions) throws Exception {
        Long stagingTableId = overwriteOptions.getStagingTableId();
        Long currentTargetId = queryTableId(connection, overwriteOptions.getTargetTable());
        return stagingTableId.equals(currentTargetId)
                && !tableExists(connection, overwriteOptions.getStagingTable());
    }

    private static boolean tableExists(Connection connection, DorisTableIdentifier table)
            throws Exception {
        String sql = "SHOW TABLES FROM " + DorisTableIdentifier.quote(table.getDatabase());
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                if (table.getTable().equals(resultSet.getString(1))) {
                    return true;
                }
            }
            return false;
        }
    }

    private static Long queryTableId(Connection connection, DorisTableIdentifier table)
            throws Exception {
        String sql =
                "SELECT TABLE_ID FROM information_schema.metadata_name_ids "
                        + "WHERE DATABASE_NAME = ? AND TABLE_NAME = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, table.getDatabase());
            statement.setString(2, table.getTable());
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getLong(1);
                }
                return null;
            }
        } catch (Exception e) {
            throw new DorisSystemException(
                    "Failed to query Doris table id for "
                            + table
                            + ". INSERT OVERWRITE staging mode requires "
                            + "information_schema.metadata_name_ids to be available.",
                    e);
        }
    }

    private static Long requireTableId(Connection connection, DorisTableIdentifier table)
            throws Exception {
        Long tableId = queryTableId(connection, table);
        if (tableId == null) {
            throw new DorisSystemException("Doris table " + table + " does not exist.");
        }
        return tableId;
    }

    private static void execute(Connection connection, String sql) throws Exception {
        try (Statement statement = connection.createStatement()) {
            LOG.info("Executing Doris overwrite SQL: {}", sql);
            statement.execute(sql);
        }
    }
}
