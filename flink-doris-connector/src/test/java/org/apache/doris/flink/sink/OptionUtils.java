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

package org.apache.doris.flink.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.PartitionDefinition;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

/** Optional Generator. */
public class OptionUtils {
    public static DorisExecutionOptions buildExecutionOptional() {
        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");

        DorisExecutionOptions.Builder builder = DorisExecutionOptions.builder();
        builder.setLabelPrefix("doris")
                .setStreamLoadProp(properties)
                .setBufferSize(8 * 1024)
                .setBufferCount(3)
                .setDeletable(true)
                .setCheckInterval(100)
                .setMaxRetries(2);
        return builder.build();
    }

    public static DorisExecutionOptions buildExecutionOptional(Properties properties) {

        DorisExecutionOptions.Builder builder = DorisExecutionOptions.builder();
        builder.setLabelPrefix("doris")
                .setStreamLoadProp(properties)
                .setBufferSize(8 * 1024)
                .setBufferCount(3)
                .setDeletable(true)
                .setCheckInterval(100)
                .setMaxRetries(2);
        return builder.build();
    }

    public static DorisReadOptions buildDorisReadOptions() {
        return dorisReadOptionsBuilder().build();
    }

    public static DorisReadOptions.Builder dorisReadOptionsBuilder() {
        DorisReadOptions.Builder builder = DorisReadOptions.builder();
        builder.setDeserializeArrowAsync(false)
                .setDeserializeQueueSize(64)
                .setExecMemLimit(2147483648L)
                .setRequestQueryTimeoutS(3600)
                .setRequestBatchSize(1000)
                .setRequestConnectTimeoutMs(10000)
                .setRequestReadTimeoutMs(10000)
                .setRequestRetries(3)
                .setRequestTabletSize(1024 * 1024);
        return builder;
    }

    public static DorisOptions buildDorisOptions() {
        DorisOptions.Builder builder = DorisOptions.builder();
        builder.setFenodes("127.0.0.1:8030")
                .setTableIdentifier("db.table")
                .setUsername("root")
                .setPassword("");
        return builder.build();
    }

    public static PartitionDefinition buildPartitionDef() {
        HashSet<Long> tabletIds = new HashSet<>(Arrays.asList(100L));
        return new PartitionDefinition("db", "table", "127.0.0.1:9060", tabletIds, "");
    }
}
