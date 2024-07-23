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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer;
import org.apache.doris.flink.utils.DateToStringConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class CDCSchemaChangeExample {

    public static void main(String[] args) throws Exception {

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(false, customConverterConfigs);

        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname("127.0.0.1")
                        .port(3306)
                        .databaseList("test") // set captured database
                        .tableList("test.t1") // set captured table
                        .username("root")
                        .password("123456")
                        .debeziumProperties(DateToStringConverter.DEFAULT_PROPS)
                        .deserializer(schema)
                        .serverTimeZone("Asia/Shanghai")
                        .includeSchemaChanges(true) // converts SourceRecord to JSON String
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(10000);

        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("test.t1")
                        .setUsername("root")
                        .setPassword("")
                        .build();

        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setLabelPrefix("label-doris" + UUID.randomUUID())
                .setStreamLoadProp(props)
                .setDeletable(true);

        DorisSink.Builder<String> builder = DorisSink.builder();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisOptions)
                .setSerializer(
                        JsonDebeziumSchemaSerializer.builder()
                                .setDorisOptions(dorisOptions)
                                .setNewSchemaChange(true)
                                .build());

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source") // .print();
                .sinkTo(builder.build());

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
