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

package org.apache.doris.flink.lookup;

import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutionPool implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionPool.class);
    private ActionWatcher readActionWatcher;
    final ArrayBlockingQueue<Get> queue;
    private AtomicBoolean started; // determine whether the executionPool is running
    private AtomicBoolean workerStated; // determine whether the worker is running
    ExecutorService actionWatcherExecutorService;
    ExecutorService workerExecutorService;
    ThreadFactory workerThreadFactory;
    ThreadFactory actionWatcherThreadFactory;
    private Worker[] workers;

    private Semaphore semaphore;

    private final int jdbcReadThreadSize;

    public ExecutionPool(DorisOptions options, DorisLookupOptions lookupOptions) {
        started = new AtomicBoolean(false);
        workerStated = new AtomicBoolean(false);
        workerThreadFactory = new DefaultThreadFactory("worker");
        actionWatcherThreadFactory = new DefaultThreadFactory("action-watcher");
        this.queue = new ArrayBlockingQueue<>(lookupOptions.getJdbcReadBatchQueueSize());
        this.readActionWatcher = new ActionWatcher(lookupOptions.getJdbcReadBatchSize());
        this.workers = new Worker[lookupOptions.getJdbcReadThreadSize()];
        for (int i = 0; i < workers.length; ++i) {
            workers[i] = new Worker(workerStated, options, lookupOptions, i);
        }
        this.jdbcReadThreadSize = lookupOptions.getJdbcReadThreadSize();
        start();
    }

    private void start() {
        if (started.compareAndSet(false, true)) {
            workerStated.set(true);
            workerExecutorService =
                    new ThreadPoolExecutor(
                            workers.length,
                            workers.length,
                            0L,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(1),
                            workerThreadFactory,
                            new ThreadPoolExecutor.AbortPolicy());
            actionWatcherExecutorService =
                    new ThreadPoolExecutor(
                            1,
                            1,
                            0L,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<>(1),
                            actionWatcherThreadFactory,
                            new ThreadPoolExecutor.AbortPolicy());
            for (int i = 0; i < workers.length; ++i) {
                workerExecutorService.execute(workers[i]);
            }
            actionWatcherExecutorService.execute(readActionWatcher);
            this.semaphore = new Semaphore(jdbcReadThreadSize);
        }
    }

    public CompletableFuture<List<Record>> get(Get get) {
        appendGet(get);
        return get.getFuture();
    }

    private void appendGet(Get get) {
        get.setFuture(new CompletableFuture<>());
        try {
            if (!queue.offer(get, 10000L, TimeUnit.MILLISECONDS)) {
                get.getFuture().completeExceptionally(new TimeoutException());
            }
        } catch (InterruptedException e) {
            get.getFuture().completeExceptionally(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (started.compareAndSet(true, false)) {
            LOG.info("close executorService");
            actionWatcherExecutorService.shutdown();
            workerExecutorService.shutdown();
            workerStated.set(false);
            this.actionWatcherExecutorService = null;
            this.workerExecutorService = null;
            this.semaphore = null;
        }
    }

    public boolean submit(GetAction action) {
        // if has semaphore, try to obtain the semaphore, otherwise return submit failure
        if (semaphore != null) {
            try {
                boolean acquire = semaphore.tryAcquire(2000L, TimeUnit.MILLISECONDS);
                if (!acquire) {
                    return false;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("get semaphore be interrupt");
            }
            action.setSemaphore(semaphore);
        }

        // try to submit to worker
        for (int i = 0; i < workers.length; ++i) {
            Worker worker = workers[i];
            if (worker.offer(action)) {
                return true;
            }
        }
        // If submit fails, it will be released, and if successful, the worker will be responsible
        // for the release
        if (semaphore != null) {
            semaphore.release();
        }
        return false;
    }

    /** monitor the query action and submit it to the worker as soon as the data arrives. */
    class ActionWatcher implements Runnable {
        private int batchSize;

        public ActionWatcher(int batchSize) {
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            LOG.info("action watcher start");
            List<Get> recordList = new ArrayList<>(batchSize);
            while (started.get()) {
                try {
                    recordList.clear();
                    Get firstGet = queue.poll(2, TimeUnit.SECONDS);
                    if (firstGet != null) {
                        recordList.add(firstGet);
                        queue.drainTo(recordList, batchSize - 1);
                        LOG.debug("fetch {} records from queue", recordList.size());
                        Map<String, List<Get>> getsByTable = new HashMap<>();
                        for (Get get : recordList) {
                            List<Get> list =
                                    getsByTable.computeIfAbsent(
                                            get.getRecord().getTableIdentifier(),
                                            (s) -> new ArrayList<>());
                            list.add(get);
                        }
                        for (Map.Entry<String, List<Get>> entry : getsByTable.entrySet()) {
                            GetAction getAction = new GetAction(entry.getValue());
                            while (!submit(getAction)) {}
                        }
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    for (Get get : recordList) {
                        if (!get.getFuture().isDone()) {
                            get.getFuture().completeExceptionally(e);
                        }
                    }
                }
            }
            LOG.info("action watcher stop");
        }

        @Override
        public String toString() {
            return "ActionWatcher{" + "batchSize=" + batchSize + '}';
        }
    }

    static class DefaultThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        DefaultThreadFactory(String name) {
            namePrefix = "pool-" + poolNumber.getAndIncrement() + "-" + name + "-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
            t.setDaemon(false);
            return t;
        }
    }
}
