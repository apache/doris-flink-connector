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

package org.apache.doris;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DorisFlinkDfSinkDemo {
    private static final Logger LOG = LoggerFactory.getLogger(DorisFlinkDfSinkDemo.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Input arguments: {}", String.join(" ", args));
        DorisArguments arguments = DorisArguments.parse(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes(arguments.getFeAddress())
                .setTableIdentifier(arguments.getTableIdentifier())
                .setUsername(arguments.getUser())
                .setPassword(arguments.getPassword())
                .setTlsOptions(arguments.toDorisTlsOptions());
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris" + UUID.randomUUID());

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        List<Tuple3<Integer, String, Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(1, "doris", 10));
        data.add(new Tuple3<>(2, "spark", 20));
        data.add(new Tuple3<>(3, "flink", 18));
        data.add(new Tuple3<>(4, "hadoop", 30));
        data.add(new Tuple3<>(5, "es", 17));
        data.add(new Tuple3<>(6, "hive", 20));
        DataStreamSource<Tuple3<Integer, String, Integer>> source = env.fromCollection(data);
        source.map(
                        (MapFunction<Tuple3<Integer, String, Integer>, String>)
                                t -> t.f0 + "\t" + t.f1 + "\t" + t.f2)
                .sinkTo(builder.build());

        env.execute("doris sink demo");
    }
}
