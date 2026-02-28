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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/** This is an example of using Doris sink with DataGen. */
public class DataGen2DorisExample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(30000);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE student_source ("
                        + "    id INT,"
                        + "    name STRING,"
                        + "    age INT"
                        + ") WITH ("
                        + "  'connector' = 'datagen',"
                        + "  'rows-per-second' = '1',"
                        + "  'fields.name.length' = '20',"
                        + "  'fields.id.min' = '1',"
                        + "  'fields.id.max' = '100000',"
                        + "  'fields.age.min' = '3',"
                        + "  'fields.age.max' = '30'"
                        + ");");

        tEnv.executeSql(
                "CREATE TABLE student_sink ("
                        + "    id INT,"
                        + "    name STRING,"
                        + "    age INT"
                        + "    ) "
                        + "    WITH ("
                        + "      'connector' = 'doris',"
                        + "      'fenodes' = '127.0.0.1:8030',"
                        + "      'table.identifier' = 'test.student',"
                        + "      'username' = 'root',"
                        + "      'password' = '',"
                        + "      'sink.label-prefix' = 'doris_label'"
                        + ")");
        tEnv.executeSql("INSERT INTO student_sink select * from student_source");
    }
}
