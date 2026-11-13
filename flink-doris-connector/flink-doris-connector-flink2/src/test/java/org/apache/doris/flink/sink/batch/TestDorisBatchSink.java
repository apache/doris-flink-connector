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

package org.apache.doris.flink.sink.batch;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.junit.Assert;
import org.junit.Test;

public class TestDorisBatchSink {

    @Test
    public void testBuild() {
        DorisBatchSink.Builder<String> builder = DorisBatchSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder
                .setFenodes("127.0.0.1:8030")
                .setTableIdentifier("db.tbl")
                .setUsername("root")
                .setPassword("");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        DorisBatchSink<String> build =
                builder.setDorisExecutionOptions(executionBuilder.build())
                        .setSerializer(new SimpleStringSerializer())
                        .setDorisOptions(dorisBuilder.build())
                        .build();

        DorisReadOptions expected = DorisReadOptions.builder().build();
        DorisReadOptions actual = build.getDorisReadOptions();
        Assert.assertEquals(expected, actual);
    }
}
