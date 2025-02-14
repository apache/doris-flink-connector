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

package org.apache.doris.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.sink.OptionUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

/** Example Tests for the {@link DorisSource}. */
@Ignore
public class DorisSourceExampleTest {

    @Test
    public void testBoundedDorisSource() throws Exception {
        DorisSource<List<?>> dorisSource =
                DorisSource.<List<?>>builder()
                        .setDorisOptions(OptionUtils.buildDorisOptions())
                        .setDorisReadOptions(OptionUtils.buildDorisReadOptions())
                        .setDeserializer(new SimpleListDeserializationSchema())
                        .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris Source")
                .addSink(new PrintSinkFunction<>());
        env.execute("Flink doris source test");
    }
}
