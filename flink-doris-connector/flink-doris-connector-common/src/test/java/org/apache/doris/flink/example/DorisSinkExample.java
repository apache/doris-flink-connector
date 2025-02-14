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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class DorisSinkExample {

    public static void main(String[] args) throws Exception {
        CSVFormatWrite();
    }

    public static void JSONFormatWrite() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(30000);
        DorisSink.Builder<String> builder = DorisSink.builder();

        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("test.student")
                        .setUsername("root")
                        .setPassword("")
                        .build();

        Properties properties = new Properties();
        properties.setProperty("read_json_by_line", "true");
        properties.setProperty("format", "json");

        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setLabelPrefix("label-doris")
                        .setDeletable(false)
                        .setBatchMode(true)
                        .setStreamLoadProp(properties)
                        .build();

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisOptions);

        List<String> data = new ArrayList<>();
        data.add("{\"id\":3,\"name\":\"Michael\",\"age\":28}");
        data.add("{\"id\":4,\"name\":\"David\",\"age\":38}");

        env.fromCollection(data).sinkTo(builder.build());
        env.execute("doris test");
    }

    public static void CSVFormatWrite() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(10000);
        env.setParallelism(1);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,
        // Time.milliseconds(30000)));
        env.setRestartStrategy(RestartStrategies.noRestart());
        DorisSink.Builder<String> builder = DorisSink.builder();
        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes("10.16.10.6:48737")
                .setTableIdentifier("test.test_flink")
                .setUsername("root")
                .setPassword("");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setLabelPrefix("label-doris")
                .setDeletable(false)
                //                .setBatchMode(true)
                .setStreamLoadProp(properties);

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
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

        env.execute("doris test");
    }
}
