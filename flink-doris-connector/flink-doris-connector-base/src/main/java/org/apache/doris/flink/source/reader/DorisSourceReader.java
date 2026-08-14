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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisSourceSplitState;
import org.apache.doris.flink.source.split.DorisStreamSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/** A {@link SourceReader} that read records from {@link DorisSourceSplit}. */
public class DorisSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                DorisSourceRecord, T, DorisSourceSplit, DorisSourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSourceReader.class);

    private final boolean offsetPublishingEnabled;
    // Accessed by the SourceReader thread and the SplitFetcher callback thread.
    private final SortedMap<Long, String> offsetsToPublish =
            Collections.synchronizedSortedMap(new TreeMap<>());
    // Only fully consumed Stream split boundaries are safe to publish.
    @Nullable private String lastFinishedStreamOffset;

    public DorisSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<DorisSourceRecord>> elementsQueue,
            DorisSourceFetcherManager fetcherManager,
            DorisReadOptions readOptions,
            RecordEmitter<DorisSourceRecord, T, DorisSourceSplitState> recordEmitter,
            SourceReaderContext context,
            Configuration config) {
        super(elementsQueue, fetcherManager, recordEmitter, config, context);
        offsetPublishingEnabled =
                readOptions.getBinlogOffsetTable() != null && context.getIndexOfSubtask() == 0;
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
        for (DorisSourceSplitState splitState : finishedSplitIds.values()) {
            DorisSourceSplit split = splitState.toDorisSourceSplit();
            if (split instanceof DorisStreamSplit) {
                lastFinishedStreamOffset = ((DorisStreamSplit) split).getEndTimestamp();
            }
        }
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    public List<DorisSourceSplit> snapshotState(long checkpointId) {
        List<DorisSourceSplit> splits = super.snapshotState(checkpointId);
        if (offsetPublishingEnabled && lastFinishedStreamOffset != null) {
            offsetsToPublish.put(checkpointId, lastFinishedStreamOffset);
        }
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        String offset = offsetsToPublish.get(checkpointId);
        if (offset == null) {
            LOG.debug("No Doris source offset to publish for checkpoint {}", checkpointId);
            return;
        }
        ((DorisSourceFetcherManager) splitFetcherManager)
                .publishOffset(
                        offset,
                        error -> {
                            if (error != null) {
                                // External offset publication does not affect Flink checkpoint
                                // correctness.
                                LOG.warn(
                                        "Failed to publish Doris source offset {} for checkpoint {}",
                                        offset,
                                        checkpointId,
                                        error);
                            } else {
                                LOG.debug(
                                        "Published Doris source offset {} for checkpoint {}",
                                        offset,
                                        checkpointId);
                            }
                            // A later checkpoint republishes lastFinishedStreamOffset after a
                            // failure, so completed publication attempts do not need to be kept.
                            removeAllOffsetsToPublishUpToCheckpoint(checkpointId);
                        });
    }

    private void removeAllOffsetsToPublishUpToCheckpoint(long checkpointId) {
        while (!offsetsToPublish.isEmpty() && offsetsToPublish.firstKey() <= checkpointId) {
            offsetsToPublish.remove(offsetsToPublish.firstKey());
        }
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
