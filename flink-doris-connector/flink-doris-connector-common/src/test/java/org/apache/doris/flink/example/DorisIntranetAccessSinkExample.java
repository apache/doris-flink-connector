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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * When the flink connector accesses doris, it parses out all surviving BE nodes according to the FE
 * address filled in.
 *
 * <p>However, when the BE node is deployed, most of the internal network IP is filled in, so the BE
 * node parsed by FE is the internal network IP. When flink is deployed on a non-intranet segment,
 * the BE node will be inaccessible on the network.
 *
 * <p>In this case, you can access the BE node on the intranet by directly configuring {@link new
 * DorisOptions.builder().setBenodes().build()}, after you configure this parameter, Flink Connector
 * will not parse all BE nodes through FE nodes.
 */
public class DorisIntranetAccessSinkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.milliseconds(30000)));

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
                .setFenodes("10.20.30.1:8030")
                .setBenodes("10.20.30.1:8040, 10.20.30.2:8040, 10.20.30.3:8040")
                .setTableIdentifier("test.test_sink")
                .setUsername("root")
                .setPassword("");

        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .disable2PC()
                .setLabelPrefix("label-doris")
                .setStreamLoadProp(properties)
                .setBufferSize(8 * 1024)
                .setBufferCount(3);

        builder.setDorisReadOptions(readOptionBuilder.build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        List<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(1, "zhangsan"));
        data.add(new Tuple2<>(2, "lisi"));
        data.add(new Tuple2<>(3, "wangwu"));
        DataStreamSource<Tuple2<Integer, String>> source = env.fromCollection(data);
        source.map((MapFunction<Tuple2<Integer, String>, String>) t -> t.f0 + "," + t.f1)
                .sinkTo(builder.build());
        env.execute("doris test");
    }
}
