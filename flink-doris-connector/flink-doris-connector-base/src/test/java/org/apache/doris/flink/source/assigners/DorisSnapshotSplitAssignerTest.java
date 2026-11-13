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

package org.apache.doris.flink.source.assigners;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.doris.flink.source.DorisSourceScanMode;
import org.apache.doris.flink.source.split.DorisSnapshotSplit;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

class DorisSnapshotSplitAssignerTest {

    @Test
    void initialSplitsOmitThriftQueryPlan() throws Exception {
        DorisSnapshotSplit split = planSingleSplit(DorisSourceScanMode.INITIAL);
        PartitionDefinition partition = split.getPartitionDefinition();

        assertThat(split.splitId()).isEqualTo("snapshot-127.0.0.1:9060-0");
        assertThat(partition.getDatabase()).isEqualTo("db");
        assertThat(partition.getTable()).isEqualTo("table");
        assertThat(partition.getBeAddress()).isEqualTo("127.0.0.1:9060");
        assertThat(partition.getTabletIds()).containsExactly(100L);
        assertThat(partition.getQueryPlan()).isEmpty();
    }

    @Test
    void snapshotSplitsKeepThriftQueryPlan() throws Exception {
        DorisSnapshotSplit split = planSingleSplit(DorisSourceScanMode.SNAPSHOT);

        assertThat(split.getPartitionDefinition().getQueryPlan()).isEqualTo("thrift-query-plan");
    }

    private static DorisSnapshotSplit planSingleSplit(DorisSourceScanMode scanMode)
            throws Exception {
        DorisOptions options = OptionUtils.buildDorisOptions();
        DorisReadOptions readOptions = DorisReadOptions.builder().setScanMode(scanMode).build();
        PartitionDefinition partition =
                new PartitionDefinition(
                        "db",
                        "table",
                        "127.0.0.1:9060",
                        Collections.singleton(100L),
                        "thrift-query-plan");
        try (MockedStatic<RestService> restService = mockStatic(RestService.class)) {
            restService
                    .when(
                            () ->
                                    RestService.parseIdentifier(
                                            eq(options.getTableIdentifier()), any()))
                    .thenReturn(new String[] {"db", "table"});
            restService
                    .when(() -> RestService.findPartitions(eq(options), eq(readOptions), any()))
                    .thenReturn(Collections.singletonList(partition));

            DorisSnapshotSplitAssigner assigner =
                    new DorisSnapshotSplitAssigner(options, readOptions);
            return assigner.remainingSplits().get(0);
        }
    }
}
