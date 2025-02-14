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

package org.apache.doris.flink.utils;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.SerializableObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;

public class MockSource extends RichParallelSourceFunction<String>
        implements CheckpointedFunction, CheckpointListener {
    private static final Logger LOG = LoggerFactory.getLogger(MockSource.class);
    private final Object blocker = new SerializableObject();
    private transient ListState<Long> state;
    private Long id = 0L;
    private int numEventsTotal;
    private volatile boolean running = true;
    private volatile long waitNextCheckpoint = 0L;
    private volatile long lastCheckpointConfirmed = 0L;

    public MockSource(int numEventsTotal) {
        this.numEventsTotal = numEventsTotal;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        int taskId = getRuntimeContext().getIndexOfThisSubtask();
        while (this.running && id < this.numEventsTotal) {
            String record = ++id + "," + taskId;
            ctx.collect(record);
            // Wait for the checkpoint to complete before sending the next record
            waitNextCheckpoint = lastCheckpointConfirmed + 1;
            synchronized (this.blocker) {
                while (this.lastCheckpointConfirmed < waitNextCheckpoint) {
                    this.blocker.wait();
                }
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        state.update(Collections.singletonList(id));
        LOG.info("snapshot state to {}", id);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        state =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>("id", TypeInformation.of(Long.class)));
        if (context.isRestored()) {
            Iterator<Long> iterator = state.get().iterator();
            while (iterator.hasNext()) {
                id += iterator.next();
            }
        }
        LOG.info("restore state from {}", id);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        synchronized (blocker) {
            this.lastCheckpointConfirmed = checkpointId;
            blocker.notifyAll();
        }
        LOG.info("checkpoint {} finished", checkpointId);
    }
}
