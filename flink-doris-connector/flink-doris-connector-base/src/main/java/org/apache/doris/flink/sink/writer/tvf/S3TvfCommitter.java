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

import org.apache.flink.api.connector.sink2.Committer;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.S3TvfOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Commits staged objects with one INSERT statement per writer and checkpoint. */
public class S3TvfCommitter implements Committer<S3TvfCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(S3TvfCommitter.class);
    private static final String COLUMNS = "columns";
    private static final String PARTIAL_COLUMNS = "partial_columns";
    private static final String FORMAT = "format";
    private static final String READ_JSON_BY_LINE = "read_json_by_line";
    private static final String ENABLE_UNIQUE_KEY_PARTIAL_UPDATE =
            "enable_unique_key_partial_update";

    private final S3TvfLoadClient loadClient;
    private final S3TvfSqlBuilder sqlBuilder;
    private final Map<String, String> sessionVariables;
    private final int maxRetries;

    public S3TvfCommitter(DorisOptions dorisOptions, DorisExecutionOptions executionOptions) {
        this(
                new JdbcS3TvfLoadClient(dorisOptions),
                executionOptions.getS3TvfOptions(),
                executionOptions.getStreamLoadProp(),
                executionOptions.getMaxRetries());
    }

    S3TvfCommitter(
            S3TvfLoadClient loadClient,
            S3TvfOptions options,
            Properties sessionProperties,
            int maxRetries) {
        this.loadClient = loadClient;
        this.sqlBuilder = new S3TvfSqlBuilder(options);
        this.sessionVariables = toSessionVariables(sessionProperties);
        this.maxRetries = maxRetries;
    }

    @Override
    public void commit(Collection<CommitRequest<S3TvfCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<S3TvfCommittable> request : requests) {
            S3TvfCommittable committable = request.getCommittable();
            try {
                if (commitOne(committable)) {
                    request.signalAlreadyCommitted();
                }
            } catch (Exception e) {
                IOException failure =
                        new IOException("Failed to commit TVF label " + committable.getLabel());
                request.signalFailedWithUnknownReason(failure);
            }
        }
    }

    private boolean commitOne(S3TvfCommittable committable) throws IOException, SQLException {
        String insertSql = sqlBuilder.buildInsertSql(committable);
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                loadClient.executeInsert(insertSql, sessionVariables);
                LOG.info("TVF load committed with label {}.", committable.getLabel());
                return false;
            } catch (SQLException e) {
                LOG.warn(
                        "TVF insert failed for label {} on attempt {} "
                                + "(SQLState={}, errorCode={}).",
                        committable.getLabel(),
                        attempt + 1,
                        e.getSQLState(),
                        e.getErrorCode());
                if (isLabelAlreadyUsed(e, committable.getLabel())) {
                    LOG.warn(
                            "TVF label {} has already been used; checking its load state.",
                            committable.getLabel());
                    if (handleLabelAlreadyUsed(committable)) {
                        return true;
                    }
                }
            }
        }
        throw new IOException("Failed to commit TVF label " + committable.getLabel());
    }

    private boolean handleLabelAlreadyUsed(S3TvfCommittable committable)
            throws SQLException, IOException {
        S3TvfLoadState state =
                loadClient.getLoadState(committable.getDatabase(), committable.getLabel());
        int retries = 0;
        while (true) {
            LOG.info("TVF label {} load state is {}.", committable.getLabel(), state);
            if (state == S3TvfLoadState.FINISHED) {
                LOG.info("TVF label {} was already committed.", committable.getLabel());
                return true;
            }
            if (state == S3TvfLoadState.CANCELLED) {
                LOG.info(
                        "TVF label {} was cancelled; the insert can be retried.",
                        committable.getLabel());
                return false;
            }
            if (state.isActive()) {
                LOG.warn(
                        "TVF label {} is {}; cancelling it before retrying the insert.",
                        committable.getLabel(),
                        state);
                try {
                    loadClient.cancelLoad(committable.getDatabase(), committable.getLabel());
                } catch (SQLException e) {
                    // The load may finish between SHOW LOAD and CANCEL LOAD. Recheck its state.
                    LOG.warn(
                            "Failed to cancel TVF label {} (SQLState={}, errorCode={}); "
                                    + "rechecking its load state.",
                            committable.getLabel(),
                            e.getSQLState(),
                            e.getErrorCode());
                }
                state = loadClient.getLoadState(committable.getDatabase(), committable.getLabel());
                LOG.info(
                        "TVF label {} load state after cancellation is {}.",
                        committable.getLabel(),
                        state);
                if (state == S3TvfLoadState.FINISHED || state == S3TvfLoadState.CANCELLED) {
                    continue;
                }
            }
            if (retries++ >= maxRetries) {
                LOG.warn(
                        "Unable to determine the final state of TVF label {} after {} retries; "
                                + "last state is {}.",
                        committable.getLabel(),
                        maxRetries,
                        state);
                break;
            }
            state = loadClient.getLoadState(committable.getDatabase(), committable.getLabel());
        }
        throw new IOException(
                "Unable to reconcile TVF label " + committable.getLabel() + " with state " + state);
    }

    private static boolean isLabelAlreadyUsed(Throwable throwable, String label) {
        String marker = "Label [" + label + "] has already been used";
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains(marker)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Map<String, String> toSessionVariables(Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> values = new HashMap<>();
        for (String name : properties.stringPropertyNames()) {
            if (!COLUMNS.equals(name)
                    && !PARTIAL_COLUMNS.equals(name)
                    && !FORMAT.equals(name)
                    && !READ_JSON_BY_LINE.equals(name)) {
                values.put(name, properties.getProperty(name));
            }
        }
        if (properties.containsKey(PARTIAL_COLUMNS)) {
            values.put(ENABLE_UNIQUE_KEY_PARTIAL_UPDATE, properties.getProperty(PARTIAL_COLUMNS));
        }
        return values;
    }

    @Override
    public void close() throws IOException {
        loadClient.close();
    }
}
