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

package org.apache.doris.flink.sink.writer;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.rest.models.RespContent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for DorisWriteMetrics. */
public class TestDorisWriteMetrics {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private SinkWriterMetricGroup metricGroup;
    private Counter numRecordsSend;
    private Counter numBytesSend;

    @Before
    public void setUp() {
        metricGroup = mock(SinkWriterMetricGroup.class);
        numRecordsSend = new SimpleCounter();
        numBytesSend = new SimpleCounter();
        when(metricGroup.getNumRecordsSendCounter()).thenReturn(numRecordsSend);
        when(metricGroup.getNumBytesSendCounter()).thenReturn(numBytesSend);
        when(metricGroup.counter(anyString())).thenAnswer(invocation -> new SimpleCounter());
        when(metricGroup.histogram(anyString(), any()))
                .thenAnswer(invocation -> new DescriptiveStatisticsHistogram(100));
    }

    @Test
    public void testSuccessFlushUpdatesStandardSinkMetrics() throws IOException {
        DorisWriteMetrics metrics = DorisWriteMetrics.of(metricGroup, "db_table");
        metrics.flush(buildRespContent("Success", 5, 100));

        Assert.assertEquals(5, numRecordsSend.getCount());
        Assert.assertEquals(100, numBytesSend.getCount());
        Assert.assertEquals(5, metrics.getTotalFlushLoadedRows().getCount());
        Assert.assertEquals(100, metrics.getTotalFlushLoadBytes().getCount());
        Assert.assertEquals(1, metrics.getTotalFlushSucceededTimes().getCount());
    }

    @Test
    public void testStandardSinkMetricsSharedAcrossTables() throws IOException {
        DorisWriteMetrics tableOneMetrics = DorisWriteMetrics.of(metricGroup, "db_table1");
        DorisWriteMetrics tableTwoMetrics = DorisWriteMetrics.of(metricGroup, "db_table2");
        tableOneMetrics.flush(buildRespContent("Success", 5, 100));
        tableTwoMetrics.flush(buildRespContent("Success", 7, 200));

        Assert.assertEquals(12, numRecordsSend.getCount());
        Assert.assertEquals(300, numBytesSend.getCount());
    }

    @Test
    public void testFailedFlushDoesNotUpdateStandardSinkMetrics() throws IOException {
        DorisWriteMetrics metrics = DorisWriteMetrics.of(metricGroup, "db_table");
        metrics.flush(buildRespContent("Fail", 0, 0));

        Assert.assertEquals(0, numRecordsSend.getCount());
        Assert.assertEquals(0, numBytesSend.getCount());
        Assert.assertEquals(1, metrics.getTotalFlushFailedTimes().getCount());
    }

    private RespContent buildRespContent(String status, long loadedRows, long loadBytes)
            throws IOException {
        String json =
                String.format(
                        "{\"Status\": \"%s\", \"NumberLoadedRows\": %d, \"LoadBytes\": %d}",
                        status, loadedRows, loadBytes);
        return OBJECT_MAPPER.readValue(json, RespContent.class);
    }
}
