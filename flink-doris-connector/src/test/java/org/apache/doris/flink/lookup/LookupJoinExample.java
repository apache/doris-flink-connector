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

package org.apache.doris.flink.lookup;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.UUID;

import static org.apache.flink.table.api.Expressions.$;

public class LookupJoinExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.enableCheckpointing(30000);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple2<Integer, String>> source =
                env.addSource(
                        new SourceFunction<Tuple2<Integer, String>>() {
                            private Integer id = 1;

                            @Override
                            public void run(SourceContext<Tuple2<Integer, String>> out)
                                    throws Exception {
                                while (true) {
                                    Tuple2<Integer, String> record =
                                            new Tuple2<>(id++, UUID.randomUUID().toString());
                                    out.collect(record);
                                    Thread.sleep(1000);
                                }
                            }

                            @Override
                            public void cancel() {}
                        });
        tEnv.createTemporaryView(
                "doris_source", source, $("id"), $("uuid"), $("process_time").proctime());

        tEnv.executeSql(
                "CREATE TABLE lookup_dim_tbl ("
                        + "  c_custkey int,"
                        + "  c_name string,"
                        + "  c_address string,"
                        + "  c_city string,"
                        + "  c_nation string,"
                        + "  c_region string,"
                        + "  c_phone string,"
                        + "  c_mktsegment string"
                        + ") "
                        + "WITH (\n"
                        + "  'connector' = 'doris',\n"
                        + "  'fenodes' = '127.0.0.1:8030',\n"
                        + "  'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',\n"
                        + "  'table.identifier' = 'ssb.customer',\n"
                        + "  'lookup.jdbc.async' = 'true',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = ''\n"
                        + ")");

        Table table =
                tEnv.sqlQuery(
                        "select a.id,a.uuid,b.c_name,b.c_nation,b.c_phone  from doris_source a "
                                + "left join lookup_dim_tbl FOR SYSTEM_TIME AS OF a.process_time b "
                                + "ON  a.id = b.c_custkey");

        tEnv.toRetractStream(table, Row.class).print();
        env.execute();
    }
}
