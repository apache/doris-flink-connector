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

package org.apache.doris.flink.source.reader;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.exception.ShouldNeverHappenException;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.SchemaUtils;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.serialization.RowBatch;
import org.apache.doris.flink.source.DorisBinlogIncrementType;
import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.doris.flink.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE;

public class DorisFlightValueReader extends ValueReader implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DorisFlightValueReader.class);
    private static final String PREFIX = "/* ApplicationName=Flink ArrowFlightSQL Query */";

    protected AdbcConnection client;
    private RootAllocator allocator;
    private AdbcDatabase database;
    protected Lock clientLock = new ReentrantLock();

    private final DorisSourceSplit split;
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private AdbcStatement statement;
    protected RowBatch rowBatch;
    protected Schema schema;
    AdbcStatement.QueryResult queryResult;
    protected ArrowReader arrowReader;
    protected AtomicBoolean eos = new AtomicBoolean(false);

    public DorisFlightValueReader(
            DorisSourceSplit split, DorisOptions options, DorisReadOptions readOptions) {
        this.split = split;
        this.options = options;
        this.readOptions = readOptions;
        try {
            initSchema();
            init();
        } catch (RuntimeException | Error failure) {
            try {
                closeFlightResources();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private void init() {
        clientLock.lock();
        try {
            this.client = openConnection();
            this.statement = this.client.createStatement();
            if (split instanceof DorisSnapshotSplit) {
                this.statement.setSqlQuery(
                        buildSnapshotSql(
                                options,
                                readOptions,
                                ((DorisSnapshotSplit) split).getPartitionDefinition()));
            } else if (split instanceof DorisStreamSplit) {
                this.statement.setSqlQuery(
                        buildIncrementalSql(
                                options,
                                readOptions,
                                (DorisStreamSplit) split,
                                readOptions.getBinlogIncrementType()));
            } else {
                throw new DorisRuntimeException("Unknown Doris split type: " + split);
            }
            this.queryResult = statement.executeQuery();
            this.arrowReader = queryResult.getReader();
        } catch (AdbcException e) {
            throw new RuntimeException(e);
        } finally {
            clientLock.unlock();
        }
        LOG.debug("Open scan result is, schema: {}.", schema);
    }

    private void initSchema() {
        try {
            this.schema = RestService.getSchema(options, readOptions, LOG);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    static String buildSnapshotSql(
            DorisOptions options, DorisReadOptions readOptions, PartitionDefinition partition) {
        String[] tableIdentifiers = parseIdentifier(options);
        String readFields =
                StringUtils.isBlank(readOptions.getReadFields())
                        ? "*"
                        : readOptions.getReadFields();

        String queryTable = quoteTable(tableIdentifiers);

        String sql = PREFIX + " SELECT " + readFields + " FROM " + queryTable;
        if (CollectionUtils.isNotEmpty(partition.getTabletIds())) {
            String tablet =
                    partition.getTabletIds().stream()
                            .sorted()
                            .map(Object::toString)
                            .collect(Collectors.joining(","));
            sql += "  TABLET(" + tablet + ") ";
        }

        if (!StringUtils.isEmpty(readOptions.getFilterQuery())) {
            sql += " WHERE " + readOptions.getFilterQuery();
        }

        if (readOptions.getRowLimit() != null) {
            sql += " LIMIT " + readOptions.getRowLimit();
        }

        LOG.info("Query SQL Sending to Doris FE is: '{}'.", sql);
        return sql;
    }

    static String buildIncrementalSql(
            DorisOptions options,
            DorisReadOptions readOptions,
            DorisStreamSplit split,
            DorisBinlogIncrementType incrementType) {
        String[] tableIdentifiers = parseIdentifier(options);
        String readFields =
                StringUtils.isBlank(readOptions.getReadFields())
                        ? "*"
                        : readOptions.getReadFields();
        return PREFIX
                + " SELECT "
                + readFields
                + ", __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__, __DORIS_BINLOG_OP__ FROM "
                + quoteTable(tableIdentifiers)
                + "@incr('startTimestamp' = "
                + quoteLiteral(split.getStartTimestamp())
                + ", 'endTimestamp' = "
                + quoteLiteral(split.getEndTimestamp())
                + ", 'incrementType' = "
                + quoteLiteral(incrementType.toSqlValue())
                + ") ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__, __DORIS_BINLOG_OP__";
    }

    private static String quoteTable(String[] identifiers) {
        return Arrays.stream(identifiers)
                .map(value -> "`" + value.replace("`", "``") + "`")
                .collect(Collectors.joining("."));
    }

    private static String quoteLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static String[] parseIdentifier(DorisOptions options) {
        try {
            return RestService.parseIdentifier(options.getTableIdentifier(), LOG);
        } catch (IllegalArgumentException e) {
            throw new DorisRuntimeException(e);
        }
    }

    private AdbcConnection openConnection() {
        final Map<String, Object> parameters = new HashMap<>();
        allocator = new RootAllocator(Integer.MAX_VALUE);
        FlightSqlDriver driver = new FlightSqlDriver(allocator);
        int flightSqlPort = resolveFlightSqlPort();
        String[] split = null;
        try {
            split = RestService.randomEndpoint(options.getFenodes(), LOG).split(":");
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Get FENode Error", e);
        }
        AdbcDriver.PARAM_URI.set(
                parameters,
                Location.forGrpcInsecure(String.valueOf(split[0]), flightSqlPort)
                        .getUri()
                        .toString());
        AdbcDriver.PARAM_USERNAME.set(parameters, options.getUsername());
        AdbcDriver.PARAM_PASSWORD.set(parameters, options.getPassword());
        try {
            database = driver.open(parameters);
            return database.connect();
        } catch (AdbcException e) {
            LOG.debug("Open Flight Connection error: {}", e.getDetails());
            throw new RuntimeException(e);
        }
    }

    private int resolveFlightSqlPort() {
        Integer configured = readOptions.getFlightSqlPort();
        Integer port =
                configured != null && configured > 0
                        ? configured
                        : RestService.tryGetArrowFlightSqlPort(options, readOptions, LOG);
        if (port == null || port <= 0) {
            throw new DorisRuntimeException("A valid Doris Flight SQL port is required");
        }
        readOptions.setFlightSqlPort(port);
        return port;
    }

    /**
     * read data and cached in rowBatch.
     *
     * @return true if hax next value
     */
    public boolean hasNext() {
        boolean hasNext = false;
        clientLock.lock();
        try {
            // Arrow data was acquired synchronously during the iterative process
            while (!eos.get() && (rowBatch == null || !rowBatch.hasNext())) {
                if (rowBatch != null) {
                    rowBatch.close();
                    rowBatch = null;
                }
                if (!eos.get()) {
                    eos.set(!arrowReader.loadNextBatch());
                    if (!eos.get()) {
                        rowBatch =
                                new RowBatch(
                                                arrowReader,
                                                SchemaUtils.convertToSchema(
                                                        this.schema,
                                                        arrowReader
                                                                .getVectorSchemaRoot()
                                                                .getSchema()),
                                                split instanceof DorisStreamSplit)
                                        .readFlightArrow();
                    }
                }
            }
            hasNext = !eos.get();
            return hasNext;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            clientLock.unlock();
        }
    }

    /**
     * get next value.
     *
     * @return next value
     */
    public DorisSourceRecord next() {
        if (!hasNext()) {
            LOG.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }
        return rowBatch.nextSourceRecord();
    }

    @Override
    public void close() throws Exception {
        clientLock.lock();
        try {
            Exception failure = null;
            if (rowBatch != null) {
                try {
                    rowBatch.close();
                } catch (RuntimeException e) {
                    failure = e;
                }
                rowBatch = null;
            }
            try {
                closeFlightResources();
            } catch (Exception resourceFailure) {
                if (failure == null) {
                    failure = resourceFailure;
                } else {
                    failure.addSuppressed(resourceFailure);
                }
            }
            if (failure != null) {
                throw failure;
            }
        } finally {
            clientLock.unlock();
        }
    }

    private void closeFlightResources() throws Exception {
        try {
            closeAll(arrowReader, statement, client, database, allocator);
        } finally {
            arrowReader = null;
            statement = null;
            client = null;
            database = null;
            allocator = null;
        }
    }

    static void closeAll(AutoCloseable... resources) throws Exception {
        Exception failure = null;
        for (AutoCloseable resource : resources) {
            if (resource == null) {
                continue;
            }
            try {
                resource.close();
            } catch (Exception closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
