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

package org.apache.doris.flink.example;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.batch.DorisBatchSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class DorisSinkBatchExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(30000);

        // env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,
        // Time.milliseconds(30000)));
        DorisSink.Builder<String> builder = DorisSink.builder();
        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();
        readOptionBuilder
                .setDeserializeArrowAsync(false)
                .setDeserializeQueueSize(64)
                .setExecMemLimit(2147483648L)
                .setRequestQueryTimeoutS(3600)
                .setRequestBatchSize(1000)
                .setRequestConnectTimeoutMs(10000)
                .setRequestReadTimeoutMs(10000)
                .setRequestRetries(3)
                .setRequestTabletSize(1024 * 1024);
        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes("10.16.10.6:28737")
                .setTableIdentifier("test.test_flink_error")
                .setUsername("root")
                .setPassword("");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label").setStreamLoadProp(properties).setDeletable(false);
        // .setBufferFlushMaxBytes(8 * 102400000)
        // .setBufferFlushMaxRows(90000)
        // .setBufferFlushIntervalMs(1000 * 10)
        // .setBatchMode(true);
        // .setWriteMode(WriteMode.STREAM_LOAD_BATCH);
        builder.setDorisReadOptions(readOptionBuilder.build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        env.addSource(
                        new SourceFunction<String>() {
                            private Long id = 0L;

                            @Override
                            public void run(SourceContext<String> out) throws Exception {
                                while (true) {
                                    id = id + 1;
                                    String record = UUID.randomUUID() + "," + id + "";
                                    out.collect(record);
                                    Thread.sleep(500);
                                }
                            }

                            @Override
                            public void cancel() {}
                        })
                .sinkTo(builder.build());

        env.execute("doris batch test");
    }

    public void testBatchFlush() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DorisBatchSink.Builder<String> builder = DorisBatchSink.builder();
        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();

        readOptionBuilder
                .setDeserializeArrowAsync(false)
                .setDeserializeQueueSize(64)
                .setExecMemLimit(2147483648L)
                .setRequestQueryTimeoutS(3600)
                .setRequestBatchSize(1000)
                .setRequestConnectTimeoutMs(10000)
                .setRequestReadTimeoutMs(10000)
                .setRequestRetries(3)
                .setRequestTabletSize(1024 * 1024);

        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes("127.0.0.1:8030")
                .setTableIdentifier("test.testd")
                .setUsername("root")
                .setPassword("");

        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();

        executionBuilder
                .setLabelPrefix("label")
                .setStreamLoadProp(properties)
                .setDeletable(false)
                .setBufferFlushMaxBytes(8 * 1024)
                .setBufferFlushMaxRows(1)
                .setBufferFlushIntervalMs(1000 * 10);

        builder.setDorisReadOptions(readOptionBuilder.build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        DataStreamSource<String> stringDataStreamSource =
                env.fromCollection(
                        Arrays.asList(
                                "1,-74159.9193252453",
                                "2,-74159.9193252453",
                                "3,-19.7004480979",
                                "4,43385.2170333507",
                                "5,-16.2602598554"));
        stringDataStreamSource.sinkTo(builder.build());

        env.execute("doris batch test");
    }
}
