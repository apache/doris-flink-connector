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

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.batch.RecordWithMeta;
import org.apache.doris.flink.sink.writer.serializer.RecordWithMetaSerializer;

import java.util.Arrays;
import java.util.Properties;

public class DorisSinkMultiTableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(15000);

        DorisSink.Builder<RecordWithMeta> builder = DorisSink.builder();
        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes("127.0.0.1:8030")
                .setTableIdentifier("")
                .setUsername("root")
                .setPassword("");

        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();

        executionBuilder
                .setLabelPrefix("label")
                .setStreamLoadProp(properties)
                .setDeletable(false)
                .setBatchMode(true)
                .setBufferFlushMaxBytes(10 * 1024 * 1024)
                .setBufferFlushMaxRows(10000)
                .setBufferFlushIntervalMs(1000 * 10);

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisBuilder.build())
                .setSerializer(new RecordWithMetaSerializer());

        RecordWithMeta record = new RecordWithMeta("test", "test_flink_tmp1", "wangwu,1");
        RecordWithMeta record1 = new RecordWithMeta("test", "test_flink_tmp", "wangwu,1");
        DataStreamSource<RecordWithMeta> stringDataStreamSource =
                env.fromCollection(Arrays.asList(record, record1));
        stringDataStreamSource.sinkTo(builder.build());

        /*
        // mock unbounded streaming source
        env.addSource(
                        new SourceFunction<RecordWithMeta>() {
                            private Long id = 1000000L;

                            @Override
                            public void run(SourceContext<RecordWithMeta> out) throws Exception {
                                while (true) {
                                    id = id + 1;
                                    RecordWithMeta record =
                                            new RecordWithMeta(
                                                    "test",
                                                    "test_flink_tmp",
                                                    UUID.randomUUID() + ",1");
                                    out.collect(record);
                                    record =
                                            new RecordWithMeta(
                                                    "test",
                                                    "test_flink_tmp1",
                                                    UUID.randomUUID() + ",1");
                                    out.collect(record);
                                    record =
                                            new RecordWithMeta(
                                                    "test",
                                                    "test_flink_tmp2",
                                                    UUID.randomUUID() + ",1");
                                    out.collect(record);
                                    Thread.sleep(1000);
                                }
                            }

                            @Override
                            public void cancel() {}
                        })
                .sinkTo(builder.build());
         **/
        env.execute("doris multi table test");
    }
}
