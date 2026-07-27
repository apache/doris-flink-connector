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

package org.apache.doris.flink.source;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.types.RowKind;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

/** Integration cases for incremental Source recovery after Flink failover. */
@RunWith(Parameterized.class)
public class DorisIncrementalSourceFailoverITCase extends AbstractDorisIncrementalITCase {
    private final FailoverType failoverType;

    public DorisIncrementalSourceFailoverITCase(FailoverType failoverType) {
        this.failoverType = failoverType;
    }

    @Parameterized.Parameters(name = "failover={0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[] {FailoverType.JM}, new Object[] {FailoverType.TM});
    }

    @Test
    public void testIncrementalSourceRecoversFromCompletedCheckpoint() throws Exception {
        String table = "tbl_incremental_failover_" + failoverType.name().toLowerCase();
        initializeIncrementalTable(table);

        Configuration configuration = new Configuration();
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);

        try (IncrementalResultCollector collector =
                startSource(table, "latest", null, configuration)) {
            completeCheckpoint(collector.getJobClient());

            ObservedRow beforeFailover = ObservedRow.of(RowKind.INSERT, 1, "before-failover");
            executeDorisSql(
                    String.format(
                            "INSERT INTO %s.%s VALUES (1,'before-failover','ignored-before')",
                            DATABASE, table));
            collector.awaitContains(beforeFailover);
            completeCheckpoint(collector.getJobClient());

            triggerFailover(
                    failoverType,
                    collector.getJobClient().getJobID(),
                    miniClusterResource.getMiniCluster(),
                    () -> insertDuringFailover(table));
            waitForJobStatus(
                    collector.getJobClient(),
                    Collections.singletonList(JobStatus.RUNNING),
                    Deadline.fromNow(DEFAULT_TIMEOUT));

            ObservedRow duringFailover = ObservedRow.of(RowKind.INSERT, 2, "during-failover");
            collector.awaitContains(duringFailover);

            executeDorisSql(
                    String.format(
                            "INSERT INTO %s.%s VALUES (3,'after-recovery','ignored-after')",
                            DATABASE, table));
            collector.awaitContainsAll(
                    Arrays.asList(
                            beforeFailover,
                            duringFailover,
                            ObservedRow.of(RowKind.INSERT, 3, "after-recovery")));
        }
    }

    private void insertDuringFailover(String table) {
        String timestamp = resolveCurrentDorisTimestamp(table);
        executeDorisSql(
                String.format(
                        "INSERT INTO %s.%s VALUES (2,'during-failover','ignored-during')",
                        DATABASE, table));
        try {
            waitForDorisTimestampAfter(table, timestamp);
        } catch (Exception e) {
            throw new RuntimeException("Doris timestamp did not advance during failover", e);
        }
    }
}
