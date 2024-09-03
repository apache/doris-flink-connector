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

import org.apache.doris.flink.backend.BackendClient;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.exception.ShouldNeverHappenException;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.SchemaUtils;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.serialization.Routing;
import org.apache.doris.flink.serialization.RowBatch;
import org.apache.doris.sdk.thrift.TScanBatchResult;
import org.apache.doris.sdk.thrift.TScanCloseParams;
import org.apache.doris.sdk.thrift.TScanNextBatchParams;
import org.apache.doris.sdk.thrift.TScanOpenParams;
import org.apache.doris.sdk.thrift.TScanOpenResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_BATCH_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_BATCH_SIZE_MAX;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_DEFAULT_CLUSTER;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_EXEC_MEM_LIMIT_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT;
import static org.apache.doris.flink.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE;

public class DorisValueReader extends ValueReader implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DorisValueReader.class);
    protected BackendClient client;
    protected Lock clientLock = new ReentrantLock();

    private PartitionDefinition partition;
    private DorisOptions options;
    private DorisReadOptions readOptions;

    protected int offset = 0;
    protected AtomicBoolean eos = new AtomicBoolean(false);
    protected RowBatch rowBatch;

    // flag indicate if support deserialize Arrow to RowBatch asynchronously
    protected Boolean deserializeArrowToRowBatchAsync;

    protected BlockingQueue<RowBatch> rowBatchBlockingQueue;
    private TScanOpenParams openParams;
    protected String contextId;
    protected Schema schema;
    protected boolean asyncThreadStarted;

    public DorisValueReader(
            PartitionDefinition partition, DorisOptions options, DorisReadOptions readOptions) {
        this.partition = partition;
        this.options = options;
        this.readOptions = readOptions;
        this.client = backendClient();
        this.deserializeArrowToRowBatchAsync =
                readOptions.getDeserializeArrowAsync() == null
                        ? DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT
                        : readOptions.getDeserializeArrowAsync();

        Integer blockingQueueSize =
                readOptions.getDeserializeQueueSize() == null
                        ? DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT
                        : readOptions.getDeserializeQueueSize();
        if (this.deserializeArrowToRowBatchAsync) {
            this.rowBatchBlockingQueue = new ArrayBlockingQueue(blockingQueueSize);
        }
        init();
    }

    private void init() {
        clientLock.lock();
        try {
            this.openParams = openParams();
            TScanOpenResult openResult = this.client.openScanner(this.openParams);
            this.contextId = openResult.getContextId();
            this.schema = SchemaUtils.convertToSchema(openResult.getSelectedColumns());
        } finally {
            clientLock.unlock();
        }
        this.asyncThreadStarted = asyncThreadStarted();
        LOG.debug("Open scan result is, contextId: {}, schema: {}.", contextId, schema);
    }

    private BackendClient backendClient() {
        try {
            return new BackendClient(new Routing(partition.getBeAddress()), readOptions);
        } catch (IllegalArgumentException e) {
            LOG.error("init backend:{} client failed,", partition.getBeAddress(), e);
            throw new DorisRuntimeException(e);
        }
    }

    private TScanOpenParams openParams() {
        TScanOpenParams params = new TScanOpenParams();
        params.cluster = DORIS_DEFAULT_CLUSTER;
        params.database = partition.getDatabase();
        params.table = partition.getTable();

        params.tablet_ids = Arrays.asList(partition.getTabletIds().toArray(new Long[] {}));
        params.opaqued_query_plan = partition.getQueryPlan();
        // max row number of one read batch
        Integer batchSize =
                readOptions.getRequestBatchSize() == null
                        ? DORIS_BATCH_SIZE_DEFAULT
                        : Math.min(readOptions.getRequestBatchSize(), DORIS_BATCH_SIZE_MAX);
        Integer queryDorisTimeout =
                readOptions.getRequestQueryTimeoutS() == null
                        ? DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT
                        : readOptions.getRequestQueryTimeoutS();
        Long execMemLimit =
                readOptions.getExecMemLimit() == null
                        ? DORIS_EXEC_MEM_LIMIT_DEFAULT
                        : readOptions.getExecMemLimit();
        params.setBatchSize(batchSize);
        params.setQueryTimeout(queryDorisTimeout);
        params.setMemLimit(execMemLimit);
        params.setUser(options.getUsername());
        params.setPasswd(options.getPassword());
        LOG.debug(
                "Open scan params is,cluster:{},database:{},table:{},tabletId:{},batch size:{},query timeout:{},execution memory limit:{},user:{},query plan: {}",
                params.getCluster(),
                params.getDatabase(),
                params.getTable(),
                params.getTabletIds(),
                params.getBatchSize(),
                params.getQueryTimeout(),
                params.getMemLimit(),
                params.getUser(),
                params.getOpaquedQueryPlan());
        return params;
    }

    protected Thread asyncThread =
            new Thread(
                    new Runnable() {
                        @Override
                        public void run() {
                            clientLock.lock();
                            try {
                                TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
                                nextBatchParams.setContextId(contextId);
                                while (!eos.get()) {
                                    nextBatchParams.setOffset(offset);
                                    TScanBatchResult nextResult = client.getNext(nextBatchParams);
                                    eos.set(nextResult.isEos());
                                    if (!eos.get()) {
                                        RowBatch rowBatch =
                                                new RowBatch(nextResult, schema).readArrow();
                                        offset += rowBatch.getReadRowCount();
                                        rowBatch.close();
                                        try {
                                            rowBatchBlockingQueue.put(rowBatch);
                                        } catch (InterruptedException e) {
                                            throw new DorisRuntimeException(e);
                                        }
                                    }
                                }
                            } finally {
                                clientLock.unlock();
                            }
                        }
                    });

    protected boolean asyncThreadStarted() {
        boolean started = false;
        if (deserializeArrowToRowBatchAsync) {
            asyncThread.start();
            started = true;
        }
        return started;
    }

    /**
     * read data and cached in rowBatch.
     *
     * @return true if hax next value
     */
    public boolean hasNext() {
        boolean hasNext = false;
        if (deserializeArrowToRowBatchAsync && asyncThreadStarted) {
            // support deserialize Arrow to RowBatch asynchronously
            if (rowBatch == null || !rowBatch.hasNext()) {
                while (!eos.get() || !rowBatchBlockingQueue.isEmpty()) {
                    if (!rowBatchBlockingQueue.isEmpty()) {
                        try {
                            rowBatch = rowBatchBlockingQueue.take();
                        } catch (InterruptedException e) {
                            throw new DorisRuntimeException(e);
                        }
                        hasNext = true;
                        break;
                    } else {
                        // wait for rowBatch put in queue or eos change
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            } else {
                hasNext = true;
            }
        } else {
            clientLock.lock();
            try {
                // Arrow data was acquired synchronously during the iterative process
                if (!eos.get() && (rowBatch == null || !rowBatch.hasNext())) {
                    if (rowBatch != null) {
                        offset += rowBatch.getReadRowCount();
                        rowBatch.close();
                    }
                    TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
                    nextBatchParams.setContextId(contextId);
                    nextBatchParams.setOffset(offset);
                    TScanBatchResult nextResult = client.getNext(nextBatchParams);
                    eos.set(nextResult.isEos());
                    if (!eos.get()) {
                        rowBatch = new RowBatch(nextResult, schema).readArrow();
                    }
                }
                hasNext = !eos.get();
            } finally {
                clientLock.unlock();
            }
        }
        return hasNext;
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
            TScanCloseParams closeParams = new TScanCloseParams();
            closeParams.setContextId(contextId);
            client.closeScanner(closeParams);
        } finally {
            clientLock.unlock();
        }
    }
}
