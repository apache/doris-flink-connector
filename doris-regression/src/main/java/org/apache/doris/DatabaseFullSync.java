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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/** Synchronizes a full database and covers inserts, updates, deletes, and schema changes. */
public class DatabaseFullSync {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseFullSync.class);
    private static String HOST = "";
    private static String TARGET_DORIS_DB = "";
    private static String USER = "";
    private static String PASSWORD = "";
    private static DorisTlsOptions TLS_OPTIONS = DorisTlsOptions.disabled();

    public static void main(String[] args) throws Exception {
        LOG.info("Input arguments: {}", String.join(" ", args));
        DorisArguments arguments = DorisArguments.parse(args);
        HOST = arguments.getFeAddress();
        TARGET_DORIS_DB = arguments.getDatabase();
        USER = arguments.getUser();
        PASSWORD = arguments.getPassword();
        TLS_OPTIONS = arguments.toDorisTlsOptions();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // Mock a MySQL CDC source.
        DataStreamSource<String> cdcSource = env.addSource(new MockMySQLSource());
        // Get the table list to synchronize.
        List<String> tableList = getTableList();
        LOG.info("sync table list:{}", tableList);
        for (String tbl : tableList) {
            DataStream<String> filterStream = filterTableData(cdcSource, tbl);
            DorisSink dorisSink = buildDorisSink(tbl);
            filterStream.sinkTo(dorisSink).name("sink " + tbl);
        }
        env.execute("Full Database Sync ");
    }

    /** Divides source records by table name. */
    private static DataStream<String> filterTableData(DataStreamSource<String> source, String table) {
        return source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String row) throws Exception {
                try {
                    JSONObject rowJson = JSON.parseObject(row);
                    JSONObject source = rowJson.getJSONObject("source");
                    String tbl = source.getString("table");
                    return table.equals(tbl);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    return false;
                }
            }
        });
    }

    /** Gets all MySQL tables that need to be synchronized. */
    private static List<String> getTableList() {
        List<String> tables = Arrays.asList("student1", "student2");
        return tables;
    }

    /** Creates a Doris sink. */
    public static DorisSink buildDorisSink(String table) {
        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes(HOST)
                .setTableIdentifier(TARGET_DORIS_DB + "." + table)
                .setUsername(USER)
                .setPassword(PASSWORD)
                .setTlsOptions(TLS_OPTIONS)
                .setAutoRedirect(true);

        DorisOptions dorisOptions = dorisBuilder.build();

        Properties pro = new Properties();
        //json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setLabelPrefix("label-" + table + UUID.randomUUID())
                .setStreamLoadProp(pro)
                .setDeletable(true)
                .enable2PC()
                .build();

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build())
                .setDorisOptions(dorisOptions);

        return builder.build();
    }
}
