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

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisSourceSplitState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/** A {@link SourceReader} that read records from {@link DorisSourceSplit}. */
public class DorisSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                List, T, DorisSourceSplit, DorisSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSourceReader.class);

    public DorisSourceReader(
            DorisOptions options,
            DorisReadOptions readOptions,
            RecordEmitter<List, T, DorisSourceSplitState> recordEmitter,
            SourceReaderContext context,
            Configuration config) {
        super(
                () -> new DorisSourceSplitReader(options, readOptions),
                recordEmitter,
                config,
                context);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, DorisSourceSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected DorisSourceSplitState initializedState(DorisSourceSplit split) {
        LOG.info("Initialized reader state for split: {}", split);
        return new DorisSourceSplitState(split);
    }

    @Override
    protected DorisSourceSplit toSplitType(String splitId, DorisSourceSplitState splitState) {
        return splitState.toDorisSourceSplit();
    }
}
