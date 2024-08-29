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

package org.apache.doris.flink.container.e2e;

import org.apache.doris.flink.exception.DorisRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A custom single-threaded executor service that manages the execution of jobs in a single thread.
 * It allows submitting a job, cancelling a currently running job, and shutting down the executor.
 */
public class CustomerSingleThreadExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(CustomerSingleThreadExecutor.class);
    private final ThreadPoolExecutor executor;
    private final AtomicReference<String> currentJobName = new AtomicReference<>();
    private final AtomicReference<Future<?>> currentJob = new AtomicReference<>();

    /**
     * Constructs a new {@code CustomerSingleThreadExecutor} instance with a single-threaded
     * executor service.
     */
    public CustomerSingleThreadExecutor() {
        this.executor =
                new ThreadPoolExecutor(
                        1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    /**
     * Submits a new job for execution.
     *
     * @param jobName The name of the job being submitted.
     * @param job The {@link Runnable} job to be executed.
     * @return A {@link Future} representing the pending results of the job.
     */
    public Future<?> submitJob(String jobName, Runnable job) {
        try {
            LOG.info("Submitting a new job. jobName={}", jobName);
            Future<?> future = executor.submit(job);
            currentJob.set(future);
            currentJobName.set(jobName);
            return future;
        } catch (Exception e) {
            LOG.error("Failed to submit job. jobName={}", jobName, e);
            throw new DorisRuntimeException(e);
        }
    }

    /**
     * Cancels the currently running job if its name matches the provided job name. The job is
     * interrupted if it is currently running.
     *
     * @param jobName The name of the job to be cancelled.
     */
    public void cancelCurrentJob(String jobName) {
        String currentName = currentJobName.get();
        Future<?> currentFuture = currentJob.get();

        if (currentFuture != null && !currentFuture.isDone() && jobName.equals(currentName)) {
            LOG.info("Cancelling the current job. jobName={}", jobName);
            boolean cancelled = currentFuture.cancel(true);
            if (cancelled) {
                LOG.info("Job successfully cancelled. jobName={}", jobName);
            } else {
                LOG.info("Job cancellation failed. jobName={}", jobName);
            }
        } else {
            LOG.info("No matching job to cancel or job already completed. jobName={}", jobName);
        }
    }

    /**
     * Shuts down the executor, preventing any new jobs from being submitted. Already submitted jobs
     * in the queue will be executed before the shutdown.
     */
    public void shutdown() {
        executor.shutdown();
    }
}
