// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.doris.flink.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.source.split.DorisSourceSplit;

import java.util.function.Consumer;

/** Runs Doris split reads and offset publication on the same I/O thread. */
public class DorisSourceFetcherManager
        extends SingleThreadFetcherManager<DorisSourceRecord, DorisSourceSplit> {

    public DorisSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<DorisSourceRecord>> elementsQueue,
            DorisOptions options,
            DorisReadOptions readOptions) {
        super(elementsQueue, () -> new DorisSourceSplitReader(options, readOptions));
    }

    public void publishOffset(String offset, Consumer<Exception> callback) {
        SplitFetcher<DorisSourceRecord, DorisSourceSplit> fetcher = getRunningFetcher();
        if (fetcher != null) {
            enqueueOffsetPublishTask(fetcher, offset, callback);
        } else {
            fetcher = createSplitFetcher();
            enqueueOffsetPublishTask(fetcher, offset, callback);
            startFetcher(fetcher);
        }
    }

    private void enqueueOffsetPublishTask(
            SplitFetcher<DorisSourceRecord, DorisSourceSplit> fetcher,
            String offset,
            Consumer<Exception> callback) {
        DorisSourceSplitReader splitReader = (DorisSourceSplitReader) fetcher.getSplitReader();
        fetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() {
                        splitReader.publishOffset(offset, callback);
                        return true;
                    }

                    @Override
                    public void wakeUp() {}
                });
    }
}
