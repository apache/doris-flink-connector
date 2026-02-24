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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import org.apache.doris.flink.rest.models.RespContent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.doris.flink.sink.LoadStatus.PUBLISH_TIMEOUT;
import static org.apache.doris.flink.sink.LoadStatus.SUCCESS;

/**
 * the metrics for Doris Writer.
 *
 * @since 1.6
 */
public class DorisWriteMetrics implements Serializable {
    private static final long serialVersionUID = 1L;
    // Window size of histogram metrics.
    private static final int HISTOGRAM_WINDOW_SIZE = 100;
    private static final List<String> DORIS_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList(SUCCESS, PUBLISH_TIMEOUT));
    private final String tableIdentifier;
    AtomicLong totalRows = new AtomicLong();
    // Counter
    private transient Counter totalFlushLoadBytes;
    private transient Counter totalFlushNumberTotalRows;
    private transient Counter totalFlushLoadedRows;
    private transient Counter totalFlushTimeMs;
    private transient Counter totalFlushSucceededTimes;
    private transient Counter totalFlushFailedTimes;

    private transient Counter totalFlushFilteredRows;
    private transient Counter totalFlushUnselectedRows;
    // Histogram
    private transient Histogram beginTxnTimeHistogramMs;
    private transient Histogram commitAndPublishTimeHistogramMs;
    private transient Histogram streamLoadPutTimeHistogramMs;
    private transient Histogram readDataTimeHistogramMs;
    private transient Histogram writeDataTimeHistogramMs;
    private transient Histogram loadTimeHistogramMs;

    private static final String COUNTER_TOTAL_FLUSH_BYTES = "totalFlushLoadBytes";
    private static final String COUNTER_TOTAL_FLUSH_ROWS = "flushTotalNumberRows";
    private static final String COUNTER_TOTAL_FLUSH_LOADED_ROWS = "totalFlushLoadedRows";
    private static final String COUNTER_TOTAL_FLUSH_COST_TIME = "totalFlushTimeMs";
    private static final String COUNTER_TOTAL_FLUSH_SUCCEEDED_TIMES_COUNT =
            "totalFlushSucceededNumber";
    private static final String COUNTER_TOTAL_FLUSH_FAILED_TIMES_COUNT = "totalFlushFailedNumber";

    private static final String COUNTER_TOTAL_FILTERED_ROWS = "totalFlushFilteredRows";
    private static final String COUNTER_TOTAL_UNSELECTED_ROWS = "totalFlushUnselectedRows";

    private static final String HISTOGRAM_BEGIN_TXN_TIME_MS = "beginTxnTimeMs";
    private static final String HISTOGRAM_STREAM_LOAD_PUT_DATA_TIME_MS = "putDataTimeMs";
    private static final String HISTOGRAM_READ_DATA_TIME_MS = "readDataTimeMs";
    private static final String HISTOGRAM_WRITE_DATA_TIME_MS = "writeDataTimeMs";
    private static final String HISTOGRAM_COMMIT_AND_PUBLISH_TIME_MS = "commitAndPublishTimeMs";
    private static final String HISTOGRAM_LOAD_TIME_MS = "loadTimeMs";

    private static final String METRIC_NAME_FORMAT = "%s_%s";

    @VisibleForTesting
    DorisWriteMetrics(SinkWriterMetricGroup sinkMetricGroup, String tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
        register(sinkMetricGroup);
    }

    public static DorisWriteMetrics of(
            SinkWriterMetricGroup sinkWriterMetricGroup, String tableIdentifier) {
        return new DorisWriteMetrics(sinkWriterMetricGroup, tableIdentifier);
    }

    public void flush(RespContent respContent) {
        if (respContent != null && DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
            flushSuccessLoad(respContent);
        } else {
            flushFailedRecord();
        }
    }

    @VisibleForTesting
    public void register(SinkWriterMetricGroup sinkMetricGroup) {
        totalFlushNumberTotalRows =
                sinkMetricGroup.counter(
                        String.format(
                                METRIC_NAME_FORMAT, tableIdentifier, COUNTER_TOTAL_FLUSH_ROWS));
        totalFlushLoadedRows =
                sinkMetricGroup.counter(
                        String.format(
                                METRIC_NAME_FORMAT,
                                tableIdentifier,
                                COUNTER_TOTAL_FLUSH_LOADED_ROWS));
        totalFlushLoadBytes =
                sinkMetricGroup.counter(
                        String.format(
                                METRIC_NAME_FORMAT, tableIdentifier, COUNTER_TOTAL_FLUSH_BYTES));
        totalFlushFilteredRows =
                sinkMetricGroup.counter(
                        String.format(
                                METRIC_NAME_FORMAT, tableIdentifier, COUNTER_TOTAL_FILTERED_ROWS));
        totalFlushUnselectedRows =
                sinkMetricGroup.counter(
                        String.format(
                                METRIC_NAME_FORMAT,
                                tableIdentifier,
                                COUNTER_TOTAL_UNSELECTED_ROWS));
        totalFlushSucceededTimes =
                sinkMetricGroup.counter(
                        String.format(
                                METRIC_NAME_FORMAT,
                                tableIdentifier,
                                COUNTER_TOTAL_FLUSH_SUCCEEDED_TIMES_COUNT));
        totalFlushFailedTimes =
                sinkMetricGroup.counter(
                        String.format(
                                METRIC_NAME_FORMAT,
                                tableIdentifier,
                                COUNTER_TOTAL_FLUSH_FAILED_TIMES_COUNT));
        totalFlushTimeMs =
                sinkMetricGroup.counter(
                        String.format(
                                METRIC_NAME_FORMAT,
                                tableIdentifier,
                                COUNTER_TOTAL_FLUSH_COST_TIME));

        loadTimeHistogramMs =
                sinkMetricGroup.histogram(
                        String.format(METRIC_NAME_FORMAT, tableIdentifier, HISTOGRAM_LOAD_TIME_MS),
                        new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
        streamLoadPutTimeHistogramMs =
                sinkMetricGroup.histogram(
                        String.format(
                                METRIC_NAME_FORMAT,
                                tableIdentifier,
                                HISTOGRAM_STREAM_LOAD_PUT_DATA_TIME_MS),
                        new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
        commitAndPublishTimeHistogramMs =
                sinkMetricGroup.histogram(
                        String.format(
                                METRIC_NAME_FORMAT,
                                tableIdentifier,
                                HISTOGRAM_COMMIT_AND_PUBLISH_TIME_MS),
                        new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
        this.beginTxnTimeHistogramMs =
                sinkMetricGroup.histogram(
                        String.format(
                                METRIC_NAME_FORMAT, tableIdentifier, HISTOGRAM_BEGIN_TXN_TIME_MS),
                        new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
        readDataTimeHistogramMs =
                sinkMetricGroup.histogram(
                        String.format(
                                METRIC_NAME_FORMAT, tableIdentifier, HISTOGRAM_READ_DATA_TIME_MS),
                        new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
        writeDataTimeHistogramMs =
                sinkMetricGroup.histogram(
                        String.format(
                                METRIC_NAME_FORMAT, tableIdentifier, HISTOGRAM_WRITE_DATA_TIME_MS),
                        new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
    }

    private void flushSuccessLoad(RespContent responseContent) {
        Optional.ofNullable(responseContent.getLoadBytes()).ifPresent(totalFlushLoadBytes::inc);
        Optional.ofNullable(responseContent.getNumberLoadedRows())
                .ifPresent(totalFlushLoadedRows::inc);
        Optional.ofNullable(responseContent.getNumberTotalRows())
                .ifPresent(totalFlushNumberTotalRows::inc);
        Optional.ofNullable(responseContent.getNumberFilteredRows())
                .ifPresent(totalFlushFilteredRows::inc);
        Optional.ofNullable(responseContent.getLoadTimeMs()).ifPresent(totalFlushTimeMs::inc);
        Optional.ofNullable(responseContent.getNumberUnselectedRows())
                .ifPresent(totalFlushUnselectedRows::inc);

        totalFlushSucceededTimes.inc();

        Optional.ofNullable(responseContent.getCommitAndPublishTimeMs())
                .ifPresent(commitAndPublishTimeHistogramMs::update);
        Optional.ofNullable(responseContent.getWriteDataTimeMs())
                .ifPresent(writeDataTimeHistogramMs::update);
        Optional.ofNullable(responseContent.getBeginTxnTimeMs())
                .ifPresent(beginTxnTimeHistogramMs::update);
        Optional.ofNullable(responseContent.getReadDataTimeMs())
                .ifPresent(readDataTimeHistogramMs::update);
        Optional.ofNullable(responseContent.getStreamLoadPutTimeMs())
                .ifPresent(streamLoadPutTimeHistogramMs::update);
        Optional.ofNullable(responseContent.getLoadTimeMs()).ifPresent(loadTimeHistogramMs::update);
    }

    private void flushFailedRecord() {
        totalFlushFailedTimes.inc();
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public Counter getTotalFlushLoadBytes() {
        return totalFlushLoadBytes;
    }

    public Counter getTotalFlushNumberTotalRows() {
        return totalFlushNumberTotalRows;
    }

    public Counter getTotalFlushLoadedRows() {
        return totalFlushLoadedRows;
    }

    public Counter getTotalFlushTimeMs() {
        return totalFlushTimeMs;
    }

    public Counter getTotalFlushSucceededTimes() {
        return totalFlushSucceededTimes;
    }

    public Counter getTotalFlushFailedTimes() {
        return totalFlushFailedTimes;
    }

    public Counter getTotalFlushFilteredRows() {
        return totalFlushFilteredRows;
    }

    public Counter getTotalFlushUnselectedRows() {
        return totalFlushUnselectedRows;
    }

    public Histogram getBeginTxnTimeHistogramMs() {
        return beginTxnTimeHistogramMs;
    }

    public Histogram getCommitAndPublishTimeHistogramMs() {
        return commitAndPublishTimeHistogramMs;
    }

    public Histogram getStreamLoadPutTimeHistogramMs() {
        return streamLoadPutTimeHistogramMs;
    }

    public Histogram getReadDataTimeHistogramMs() {
        return readDataTimeHistogramMs;
    }

    public Histogram getWriteDataTimeHistogramMs() {
        return writeDataTimeHistogramMs;
    }

    public Histogram getLoadTimeHistogramMs() {
        return loadTimeHistogramMs;
    }

    @VisibleForTesting
    public void setTotalFlushLoadBytes(Counter totalFlushLoadBytes) {
        this.totalFlushLoadBytes = totalFlushLoadBytes;
    }

    @VisibleForTesting
    public void setTotalFlushNumberTotalRows(Counter totalFlushNumberTotalRows) {
        this.totalFlushNumberTotalRows = totalFlushNumberTotalRows;
    }

    @VisibleForTesting
    public void setTotalFlushLoadedRows(Counter totalFlushLoadedRows) {
        this.totalFlushLoadedRows = totalFlushLoadedRows;
    }

    @VisibleForTesting
    public void setTotalFlushTimeMs(Counter totalFlushTimeMs) {
        this.totalFlushTimeMs = totalFlushTimeMs;
    }

    @VisibleForTesting
    public void setTotalFlushSucceededTimes(Counter totalFlushSucceededTimes) {
        this.totalFlushSucceededTimes = totalFlushSucceededTimes;
    }

    @VisibleForTesting
    public void setTotalFlushFailedTimes(Counter totalFlushFailedTimes) {
        this.totalFlushFailedTimes = totalFlushFailedTimes;
    }

    @VisibleForTesting
    public void setTotalFlushFilteredRows(Counter totalFlushFilteredRows) {
        this.totalFlushFilteredRows = totalFlushFilteredRows;
    }

    @VisibleForTesting
    public void setTotalFlushUnselectedRows(Counter totalFlushUnselectedRows) {
        this.totalFlushUnselectedRows = totalFlushUnselectedRows;
    }

    @VisibleForTesting
    public void setBeginTxnTimeHistogramMs(Histogram beginTxnTimeHistogramMs) {
        this.beginTxnTimeHistogramMs = beginTxnTimeHistogramMs;
    }

    @VisibleForTesting
    public void setCommitAndPublishTimeHistogramMs(Histogram commitAndPublishTimeHistogramMs) {
        this.commitAndPublishTimeHistogramMs = commitAndPublishTimeHistogramMs;
    }

    @VisibleForTesting
    public void setStreamLoadPutTimeHistogramMs(Histogram streamLoadPutTimeHistogramMs) {
        this.streamLoadPutTimeHistogramMs = streamLoadPutTimeHistogramMs;
    }

    @VisibleForTesting
    public void setReadDataTimeHistogramMs(Histogram readDataTimeHistogramMs) {
        this.readDataTimeHistogramMs = readDataTimeHistogramMs;
    }

    @VisibleForTesting
    public void setWriteDataTimeHistogramMs(Histogram writeDataTimeHistogramMs) {
        this.writeDataTimeHistogramMs = writeDataTimeHistogramMs;
    }

    @VisibleForTesting
    public void setLoadTimeHistogramMs(Histogram loadTimeHistogramMs) {
        this.loadTimeHistogramMs = loadTimeHistogramMs;
    }
}
