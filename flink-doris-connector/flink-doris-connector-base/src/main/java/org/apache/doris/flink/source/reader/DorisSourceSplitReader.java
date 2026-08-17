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

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.connection.SimpleJdbcConnectionProvider;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisSplitRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Consumer;

/** The {@link SplitReader} implementation for the doris source. */
public class DorisSourceSplitReader implements SplitReader<DorisSourceRecord, DorisSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSourceSplitReader.class);

    private final Queue<DorisSourceSplit> splits;
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    @Nullable private final DorisOffsetPublisher offsetPublisher;
    private ValueReader valueReader;
    private String currentSplitId;

    public DorisSourceSplitReader(DorisOptions options, DorisReadOptions readOptions) {
        this.options = options;
        this.readOptions = readOptions;
        this.splits = new ArrayDeque<>();
        this.offsetPublisher =
                readOptions.getBinlogOffsetTable() == null
                        ? null
                        : new DorisOffsetPublisher(
                                new SimpleJdbcConnectionProvider(options),
                                readOptions.getBinlogOffsetTable(),
                                readOptions.getBinlogConsumerId());
    }

    @Override
    public RecordsWithSplitIds<DorisSourceRecord> fetch() throws IOException {
        checkSplitOrStartNext();

        if (!valueReader.hasNext()) {
            return finishSplit();
        }
        return DorisSplitRecords.forRecords(currentSplitId, valueReader);
    }

    private void checkSplitOrStartNext() throws IOException {
        if (valueReader != null) {
            return;
        }
        final DorisSourceSplit nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new IOException("Cannot fetch from another split - no split remaining");
        }
        currentSplitId = nextSplit.splitId();
        LOG.info("Fetch a new split {}", nextSplit);
        valueReader = ValueReader.createReader(nextSplit, options, readOptions, LOG);
    }

    private DorisSplitRecords finishSplit() {
        if (valueReader != null) {
            try {
                valueReader.close();
            } catch (Exception e) {
                LOG.warn(
                        "Failed to close value reader for split {}: {}",
                        currentSplitId,
                        e.getMessage());
            }
            valueReader = null;
        }

        LOG.info("Finished reading split {}", currentSplitId);
        final DorisSplitRecords finishRecords = DorisSplitRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<DorisSourceSplit> splitsChange) {
        LOG.info("Handling split change {}", splitsChange);
        splits.addAll(splitsChange.splits());
    }

    @Override
    public void wakeUp() {}

    void publishOffset(String offset, Consumer<Exception> callback) {
        if (offsetPublisher != null) {
            offsetPublisher.publish(offset, callback);
        }
    }

    @Override
    public void close() throws Exception {
        if (valueReader != null) {
            valueReader.close();
        }
        if (offsetPublisher != null) {
            offsetPublisher.close();
        }
    }
}
