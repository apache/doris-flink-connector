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

/**
 * A thread pool executor that ensures only one job is executed at a time.
 *
 * <p>If a job is running, new jobs will wait in the queue until the current job is completed.
 */
public class CustomerSingleThreadExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(CustomerSingleThreadExecutor.class);
    private final ThreadPoolExecutor executor;
    private final long timeoutMillis = TimeUnit.HOURS.toMillis(1);
    private String currentJobName;
    private Future<?> currentJob;

    public CustomerSingleThreadExecutor() {
        this.executor =
                new ThreadPoolExecutor(
                        1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    }

    /**
     * Submits a job to the executor. If there is already a job running, the new job will wait in
     * the queue until the current job is completed or the current job times out.
     *
     * @param jobName The name of the job to be submitted.
     * @param job The Runnable representing the job.
     * @return A Future representing the pending completion of the job.
     */
    public synchronized Future<?> submitJob(String jobName, Runnable job) {
        // Wait for the current task to complete, with a timeout of 1 hour
        long startTime = System.currentTimeMillis();
        while (currentJob != null && (!currentJob.isDone() || !currentJob.isCancelled())) {
            try {
                long elapsed = System.currentTimeMillis() - startTime;
                long remainingTime = timeoutMillis - elapsed;

                if (remainingTime <= 0) {
                    LOG.warn(
                            "Current job exceeded the maximum timeout of 1 hour and will be canceled. jobName={}",
                            currentJobName);
                    cancelCurrentJob(currentJobName);
                    break;
                }

                wait(remainingTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DorisRuntimeException(
                        "Thread was interrupted while waiting for the current job to complete.", e);
            }
        }

        LOG.info("Submitting a new job. jobName={}", jobName);
        currentJob =
                executor.submit(
                        () -> {
                            try {
                                job.run();
                            } finally {
                                synchronized (this) {
                                    // Only notify when the job has been cancelled
                                    if (currentJob.isCancelled() || currentJob.isDone()) {
                                        notifyAll();
                                    }
                                }
                            }
                        });
        currentJobName = jobName;
        return currentJob;
    }

    /**
     * Cancels the currently running job if its name matches the provided job name. The job is
     * interrupted if it is currently running.
     *
     * @param jobName The name of the job to be cancelled.
     */
    public synchronized void cancelCurrentJob(String jobName) {
        if (currentJob != null && !currentJob.isDone() && currentJobName.equals(jobName)) {
            currentJob.cancel(true);
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
