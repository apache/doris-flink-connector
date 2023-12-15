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

package org.apache.doris.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.LoadConstants;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;

import java.util.Properties;
import java.util.UUID;

public class DorisSinkExampleRowData {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.enableCheckpointing(10000);
        env.setParallelism(1);
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.milliseconds(30000)));
        DorisSink.Builder<RowData> builder = DorisSink.builder();

        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        // properties.setProperty("read_json_by_line", "true");
        // properties.setProperty("format", "json");
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes("127.0.0.1:8030")
                .setTableIdentifier("db.tbl")
                .setUsername("root")
                .setPassword("");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix(UUID.randomUUID().toString()).setStreamLoadProp(properties);

        // flink rowdata‘s schema
        String[] fields = {"name", "age"};
        DataType[] types = {DataTypes.VARCHAR(256), DataTypes.INT()};

        builder.setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(
                        RowDataSerializer.builder() // serialize according to rowdata
                                .setType(LoadConstants.CSV) // .setType(LoadConstants.CSV)
                                .setFieldDelimiter(",")
                                .setFieldNames(fields) // .setFieldDelimiter(",")
                                .setFieldType(types)
                                .build())
                .setDorisOptions(dorisBuilder.build());

        // mock rowdata source
        DataStream<RowData> source =
                env.fromElements("")
                        .flatMap(
                                new FlatMapFunction<String, RowData>() {
                                    @Override
                                    public void flatMap(String s, Collector<RowData> out)
                                            throws Exception {
                                        GenericRowData genericRowData = new GenericRowData(2);
                                        genericRowData.setField(
                                                0, StringData.fromString("beijing"));
                                        genericRowData.setField(1, 123);
                                        out.collect(genericRowData);

                                        GenericRowData genericRowData2 = new GenericRowData(2);
                                        genericRowData2.setField(
                                                0, StringData.fromString("shanghai"));
                                        genericRowData2.setField(1, 1234);
                                        out.collect(genericRowData2);
                                    }
                                });

        source.sinkTo(builder.build());
        env.execute("doris test");
    }
}
