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

package org.apache.doris.flink.flight.arrow;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.flight.split.DorisFlightSourceSplit;
import org.apache.doris.flink.flight.split.DorisFlightSplitRecords;
import org.apache.doris.flink.rest.RestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/** The {@link SplitReader} implementation for the doris source. */
public class DorisFlightSourceSplitReader implements SplitReader<List, DorisFlightSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisFlightSourceSplitReader.class);

    private final Queue<DorisFlightSourceSplit> splits;
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private DorisFlightValueReader valueReader;
    private String currentSplitId;
    protected BufferAllocator allocator;
    private AdbcConnection client;

    public DorisFlightSourceSplitReader(DorisOptions options, DorisReadOptions readOptions) {
        this.options = options;
        this.readOptions = readOptions;
        this.splits = new ArrayDeque<>();
        this.client = backendClient();
    }

    @Override
    public RecordsWithSplitIds<List> fetch() throws IOException {
        try {
            checkSplitOrStartNext();
        } catch (DorisException e) {
            throw new RuntimeException(e);
        }

        if (!valueReader.hasNext()) {
            return finishSplit();
        }
        return DorisFlightSplitRecords.forRecords(currentSplitId, valueReader);
    }

    private void checkSplitOrStartNext() throws IOException, DorisException {
        if (valueReader != null) {
            return;
        }
        final DorisFlightSourceSplit nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new IOException("Cannot fetch from another split - no split remaining");
        }
        currentSplitId = nextSplit.splitId();
        valueReader =
                new DorisFlightValueReader(
                        nextSplit.getPartitionDefinition(),
                        options,
                        readOptions,
                        client,
                        RestService.getSchema(options, readOptions, LOG));
    }

    private DorisFlightSplitRecords finishSplit() {
        final DorisFlightSplitRecords finishRecords =
                DorisFlightSplitRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }

    private AdbcConnection backendClient() {
        final Map<String, Object> parameters = new HashMap<>();
        this.allocator = new RootAllocator(Integer.MAX_VALUE);
        FlightSqlDriver driver = new FlightSqlDriver(allocator);
        String[] split = options.getFenodes().split(":");
        AdbcDriver.PARAM_URI.set(
                parameters,
                Location.forGrpcInsecure(
                                String.valueOf(split[0]),
                                Integer.parseInt(readOptions.getFlightSqlPort()))
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

    @Override
    public void handleSplitsChanges(SplitsChange<DorisFlightSourceSplit> splitsChange) {
        LOG.debug("Handling split change {}", splitsChange);
        splits.addAll(splitsChange.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        if (valueReader != null) {
            valueReader.close();
        }
        if (client != null) {
            client.close();
        }
    }
}
