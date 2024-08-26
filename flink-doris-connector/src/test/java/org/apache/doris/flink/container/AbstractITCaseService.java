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
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeoutException;

public abstract class AbstractITCaseService extends AbstractContainerTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractITCaseService.class);

    protected static void waitForJobStatus(
            JobClient client, List<JobStatus> expectedStatus, Deadline deadline) throws Exception {
        waitUntilCondition(
                () -> {
                    JobStatus currentStatus = (JobStatus) client.getJobStatus().get();
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

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(2)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

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
}
