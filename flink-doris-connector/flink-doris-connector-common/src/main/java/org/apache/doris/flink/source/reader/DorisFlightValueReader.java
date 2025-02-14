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
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.exception.ShouldNeverHappenException;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.SchemaUtils;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.serialization.RowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
    protected Lock clientLock = new ReentrantLock();

    private final PartitionDefinition partition;
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private AdbcStatement statement;
    protected RowBatch rowBatch;
    protected Schema schema;
    AdbcStatement.QueryResult queryResult;
    protected ArrowReader arrowReader;
    protected AtomicBoolean eos = new AtomicBoolean(false);

    public DorisFlightValueReader(
            PartitionDefinition partition, DorisOptions options, DorisReadOptions readOptions) {
        this.partition = partition;
        this.options = options;
        this.readOptions = readOptions;
        initSchema();
        this.client = openConnection();
        init();
    }

    private void init() {
        clientLock.lock();
        try {
            this.statement = this.client.createStatement();
            this.statement.setSqlQuery(parseFlightSql(readOptions, options, partition, LOG));
            this.queryResult = statement.executeQuery();
            this.arrowReader = queryResult.getReader();
        } catch (AdbcException | DorisException e) {
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

    private String parseFlightSql(
            DorisReadOptions readOptions,
            DorisOptions options,
            PartitionDefinition partition,
            Logger logger)
            throws IllegalArgumentException {
        String[] tableIdentifiers =
                RestService.parseIdentifier(options.getTableIdentifier(), logger);
        String readFields =
                StringUtils.isBlank(readOptions.getReadFields())
                        ? "*"
                        : readOptions.getReadFields();

        String queryTable =
                Arrays.stream(tableIdentifiers)
                        .map(v -> "`" + v + "`")
                        .collect(Collectors.joining("."));

        String sql = PREFIX + " SELECT " + readFields + " FROM " + queryTable;
        if (CollectionUtils.isNotEmpty(partition.getTabletIds())) {
            String tablet =
                    partition.getTabletIds().stream()
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

        logger.info("Query SQL Sending to Doris FE is: '{}'.", sql);
        return sql;
    }

    private AdbcConnection openConnection() {
        final Map<String, Object> parameters = new HashMap<>();
        RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        FlightSqlDriver driver = new FlightSqlDriver(allocator);
        String[] split = null;
        try {
            split = RestService.randomEndpoint(options.getFenodes(), LOG).split(":");
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Get FENode Error", e);
        }
        AdbcDriver.PARAM_URI.set(
                parameters,
                Location.forGrpcInsecure(String.valueOf(split[0]), readOptions.getFlightSqlPort())
                        .getUri()
                        .toString());
        AdbcDriver.PARAM_USERNAME.set(parameters, options.getUsername());
        AdbcDriver.PARAM_PASSWORD.set(parameters, options.getPassword());
        try {
            AdbcDatabase adbcDatabase = driver.open(parameters);
            return adbcDatabase.connect();
        } catch (AdbcException e) {
            LOG.debug("Open Flight Connection error: {}", e.getDetails());
            throw new RuntimeException(e);
        }
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
            if (!eos.get() && (rowBatch == null || !rowBatch.hasNext())) {
                if (!eos.get()) {
                    eos.set(!arrowReader.loadNextBatch());
                    rowBatch =
                            new RowBatch(
                                            arrowReader,
                                            SchemaUtils.convertToSchema(
                                                    this.schema,
                                                    arrowReader.getVectorSchemaRoot().getSchema()))
                                    .readFlightArrow();
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
    public List next() {
        if (!hasNext()) {
            LOG.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }
        return rowBatch.next();
    }

    @Override
    public void close() throws Exception {
        clientLock.lock();
        try {
            if (rowBatch != null) {
                rowBatch.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (client != null) {
                client.close();
            }
        } finally {
            clientLock.unlock();
        }
    }
}
