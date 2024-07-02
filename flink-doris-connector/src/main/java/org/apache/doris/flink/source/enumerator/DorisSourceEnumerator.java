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

package org.apache.doris.flink.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.source.assigners.DorisSplitAssigner;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A SplitEnumerator implementation for bounded / batch {@link DorisSource} input.
 *
 * <p>This enumerator takes all backend tablets and assigns them to the readers. Once tablets are
 * processed, the source is finished.
 */
public class DorisSourceEnumerator
        implements SplitEnumerator<DorisSourceSplit, PendingSplitsCheckpoint> {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSourceEnumerator.class);
    private final SplitEnumeratorContext<DorisSourceSplit> context;

    private final DorisSplitAssigner splitAssigner;

    public DorisSourceEnumerator(
            SplitEnumeratorContext<DorisSourceSplit> context, DorisSplitAssigner splitAssigner) {
        this.context = context;
        this.splitAssigner = checkNotNull(splitAssigner);
    }

    @Override
    public void start() {
        // no resources to start
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String hostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        final Optional<DorisSourceSplit> nextSplit = splitAssigner.getNext(hostname);
        if (nextSplit.isPresent()) {
            final DorisSourceSplit split = nextSplit.get();
            context.assignSplit(split, subtaskId);
            LOG.info("Assigned split to subtask {} : {}", subtaskId, split);
        } else {
            context.signalNoMoreSplits(subtaskId);
            LOG.info("No more splits available for subtask {}", subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<DorisSourceSplit> splits, int subtaskId) {
        LOG.info("Doris Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // do nothing
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) throws Exception {
        return splitAssigner.snapshotState(checkpointId);
    }

    @Override
    public void close() throws IOException {
        // no resources to close
    }
}
