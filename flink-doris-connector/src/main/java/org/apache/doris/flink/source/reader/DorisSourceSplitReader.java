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
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisSplitRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/** The {@link SplitReader} implementation for the doris source. */
public class DorisSourceSplitReader implements SplitReader<List, DorisSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSourceSplitReader.class);

    private final Queue<DorisSourceSplit> splits;
    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private ValueReader valueReader;
    private String currentSplitId;

    public DorisSourceSplitReader(DorisOptions options, DorisReadOptions readOptions) {
        this.options = options;
        this.readOptions = readOptions;
        this.splits = new ArrayDeque<>();
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
        return DorisSplitRecords.forRecords(currentSplitId, valueReader);
    }

    private void checkSplitOrStartNext() throws IOException, DorisException {
        if (valueReader != null) {
            return;
        }
        final DorisSourceSplit nextSplit = splits.poll();
        if (nextSplit == null) {
            throw new IOException("Cannot fetch from another split - no split remaining");
        }
        currentSplitId = nextSplit.splitId();
        valueReader =
                ValueReader.createReader(
                        nextSplit.getPartitionDefinition(), options, readOptions, LOG);
    }

    private DorisSplitRecords finishSplit() {
        final DorisSplitRecords finishRecords = DorisSplitRecords.finishedSplit(currentSplitId);
        currentSplitId = null;
        return finishRecords;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<DorisSourceSplit> splitsChange) {
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
    }
}
