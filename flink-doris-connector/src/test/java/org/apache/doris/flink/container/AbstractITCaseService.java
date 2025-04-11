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

package org.apache.doris.flink.container;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public abstract class AbstractITCaseService extends AbstractContainerTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractITCaseService.class);

    protected static void waitForJobStatus(
            JobClient client, List<JobStatus> expectedStatus, Deadline deadline) throws Exception {
        waitUntilCondition(
                () -> {
                    JobStatus currentStatus;
                    try {
                        currentStatus = (JobStatus) client.getJobStatus().get();
                    } catch (IllegalStateException e) {
                        LOG.warn("Failed to get state, cause " + e.getMessage());
                        currentStatus = JobStatus.FINISHED;
                    }

                    if (expectedStatus.contains(currentStatus)) {
                        return true;
                    } else if (currentStatus.isTerminalState()) {
                        try {
                            client.getJobExecutionResult().get();
                        } catch (Exception var4) {
                            throw new IllegalStateException(
                                    String.format(
                                            "Job has entered %s state, but expecting %s",
                                            currentStatus, expectedStatus),
                                    var4);
                        }

                        throw new IllegalStateException(
                                String.format(
                                        "Job has entered a terminal state %s, but expecting %s",
                                        currentStatus, expectedStatus));
                    } else {
                        return false;
                    }
                },
                deadline,
                100L,
                "Condition was not met in given timeout.");
    }

    protected static void waitUntilCondition(
            SupplierWithException<Boolean, Exception> condition,
            Deadline timeout,
            long retryIntervalMillis,
            String errorMsg)
            throws Exception {
        while (timeout.hasTimeLeft() && !(Boolean) condition.get()) {
            long timeLeft = Math.max(0L, timeout.timeLeft().toMillis());
            Thread.sleep(Math.min(retryIntervalMillis, timeLeft));
        }

        if (!timeout.hasTimeLeft()) {
            throw new TimeoutException(errorMsg);
        }
    }

    /** The type of failover. */
    protected enum FailoverType {
        TM,
        JM,
        NONE
    }

    protected static void triggerFailover(
            FailoverType type, JobID jobId, MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        LOG.info("Will job trigger failover. type={}, jobId={}", type, jobId);
        switch (type) {
            case TM:
                restartTaskManager(miniCluster, afterFailAction);
                break;
            case JM:
                triggerJobManagerFailover(jobId, miniCluster, afterFailAction);
                break;
            case NONE:
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    protected static void restartTaskManager(MiniCluster miniCluster, Runnable afterFailAction)
            throws Exception {
        LOG.info("flink cluster will terminate task manager.");
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        LOG.info("flink cluster will start task manager.");
        miniCluster.startTaskManager();
    }

    protected static void triggerJobManagerFailover(
            JobID jobId, MiniCluster miniCluster, Runnable afterFailAction) throws Exception {
        LOG.info("flink cluster will revoke job master leadership. jobId={}", jobId);
        final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        LOG.info("flink cluster will grant job master leadership. jobId={}", jobId);
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    protected void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    protected JobStatus getFlinkJobStatus(JobClient jobClient) {
        JobStatus jobStatus;
        try {
            jobStatus = jobClient.getJobStatus().get();
        } catch (IllegalStateException e) {
            LOG.warn("Failed to get state, cause " + e.getMessage());
            jobStatus = JobStatus.FINISHED;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return jobStatus;
    }

    protected void faultInjectionOpen() throws IOException {
        String pointName = "FlushToken.submit_flush_error";
        String apiUrl =
                String.format(
                        "http://%s/api/debug_point/add/%s",
                        dorisContainerService.getBenodes(), pointName);
        HttpPost httpPost = new HttpPost(apiUrl);
        httpPost.addHeader(
                HttpHeaders.AUTHORIZATION,
                auth(dorisContainerService.getUsername(), dorisContainerService.getPassword()));
        try (CloseableHttpClient httpClient = HttpClients.custom().build()) {
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                int statusCode = response.getStatusLine().getStatusCode();
                String reason = response.getStatusLine().toString();
                if (statusCode == 200 && response.getEntity() != null) {
                    LOG.info("Debug point response {}", EntityUtils.toString(response.getEntity()));
                } else {
                    LOG.info("Debug point failed, statusCode: {}, reason: {}", statusCode, reason);
                }
            }
        }
    }

    protected void faultInjectionClear() throws IOException {
        String apiUrl =
                String.format(
                        "http://%s/api/debug_point/clear", dorisContainerService.getBenodes());
        HttpPost httpPost = new HttpPost(apiUrl);
        httpPost.addHeader(
                HttpHeaders.AUTHORIZATION,
                auth(dorisContainerService.getUsername(), dorisContainerService.getPassword()));
        try (CloseableHttpClient httpClient = HttpClients.custom().build()) {
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                int statusCode = response.getStatusLine().getStatusCode();
                String reason = response.getStatusLine().toString();
                if (statusCode == 200 && response.getEntity() != null) {
                    LOG.info("Debug point response {}", EntityUtils.toString(response.getEntity()));
                } else {
                    LOG.info("Debug point failed, statusCode: {}, reason: {}", statusCode, reason);
                }
            }
        }
    }

    protected String auth(String user, String password) {
        final String authInfo = user + ":" + password;
        byte[] encoded = Base64.encodeBase64(authInfo.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    protected enum FaultType {
        RESTART_FAILURE,
        STREAM_LOAD_FAILURE,
        CHECKPOINT_FAILURE
    }
}
