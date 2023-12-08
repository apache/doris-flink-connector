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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.UUID;

public class DorisSinkMapSQLExample {

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("CREATE TABLE map_dori_source (\n" +
                "  `id` int,\n" +
                "  `c_1` map<BOOLEAN,BOOLEAN>, \n" +
                "  `c_2` map<TINYINT,TINYINT>, \n" +
                "  `c_3` map<SMALLINT,SMALLINT>, \n" +
                "  `c_4` map<INT,INT> ,\n" +
                "  `c_5` map<BIGINT,BIGINT>, \n" +
                "  `c_6` map<BIGINT,BIGINT>, \n" +
                "  `c_7` map<FLOAT,FLOAT>, \n" +
                "  `c_8` map<DOUBLE,DOUBLE>, \n" +
                "  `c_9` map<DECIMAL(4,2),DECIMAL(4,2)>, \n" +
                "  `c_10` map<DATE,DATE>, \n" +
                "  `c_11` map<DATE,DATE>, \n" +
                "  `c_12` map<TIMESTAMP,TIMESTAMP>, \n" +
                "  `c_13` map<TIMESTAMP,TIMESTAMP>, \n" +
                "  `c_14` map<TIMESTAMP_LTZ,TIMESTAMP_LTZ>, \n" +
                "  `c_15` map<TIMESTAMP_LTZ,TIMESTAMP_LTZ>, \n" +
                "  `c_16` map<TIME,TIME>, \n" +
                "  `c_17` map<CHAR(10),CHAR(10)>, \n" +
                "  `c_18` map<VARCHAR(256),VARCHAR(256)>, \n" +
                "  `c_19` map<BINARY,BINARY>, \n" +
                "  `c_20` map<VARBINARY,VARBINARY> \n" +
                "  " +
                ") WITH (\n" +
                "  'connector' = 'datagen', \n" +
                "  'number-of-rows' = '5' , \n" +
                "  'fields.c_7.key.min' = '1', \n" +
                "  'fields.c_7.value.min' = '1', \n" +
                "  'fields.c_7.key.max' = '10', \n" +
                "  'fields.c_7.value.max' = '10', \n" +
                "  'fields.c_8.key.min' = '1', \n" +
                "  'fields.c_8.value.min' = '1', \n" +
                "  'fields.c_8.key.max' = '10', \n" +
                "  'fields.c_8.value.max' = '10', \n" +
                "  'fields.c_17.key.length' = '10', \n" +
                "  'fields.c_17.value.length' = '10', \n" +
                "  'fields.c_18.key.length' = '10', \n" +
                "  'fields.c_18.value.length' = '10', \n" +
                "  'fields.c_19.key.length' = '10', \n" +
                "  'fields.c_20.value.length' = '10'\n" +
                ");");



        tEnv.executeSql("CREATE TABLE map_doris_sink (\n" +
                "  `id` int,\n" +
                "  `c_1` map<BOOLEAN,BOOLEAN>, \n" +
                "  `c_2` map<TINYINT,TINYINT>, \n" +
                "  `c_3` map<SMALLINT,SMALLINT>, \n" +
                "  `c_4` map<INT,INT> ,\n" +
                "  `c_5` map<BIGINT,BIGINT>, \n" +
                "  `c_6` map<BIGINT,BIGINT>, \n" +
                "  `c_7` map<FLOAT,FLOAT>, \n" +
                "  `c_8` map<DOUBLE,DOUBLE>, \n" +
                "  `c_9` map<DECIMAL(4,2),DECIMAL(4,2)>, \n" +
                "  `c_10` map<DATE,DATE>, \n" +
                "  `c_11` map<DATE,DATE>, \n" + //map<string,string>
                "  `c_12` map<TIMESTAMP,TIMESTAMP>, \n" +
                "  `c_13` map<timestamp,timestamp>, \n" + //map<timestamp,timestamp>
                "  `c_14` map<TIMESTAMP,TIMESTAMP>, \n" +//map<timestamp_ltz,timestamp_ltz>
                "  `c_15` map<timestamp_ltz,timestamp_ltz>, \n" +//map<timestamp_ltz,timestamp_ltz>
                "  `c_16` map<time,time>, \n" +//map<time,time>
                "  `c_17` map<CHAR(10),CHAR(10)>, \n" +
                "  `c_18` map<VARCHAR(256),VARCHAR(256)>, \n" +
                "  `c_19` map<VARBINARY,VARBINARY>, \n" +//map<VARBINARY,VARBINARY>
                "  `c_20` map<VARBINARY,VARBINARY> \n" +//map<VARBINARY,VARBINARY>
                "  " +
                ") WITH (\n" +
                "  'connector' = 'doris',\n" +
                        "  'fenodes' = '127.0.0.1:8030',\n" +
                        "  'table.identifier' = 'test.all_map_type',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '123456',\n" +
                        "  'sink.label-prefix' = 'doris_label_map"  + UUID.randomUUID() + "'" +
                ");");

        tEnv.executeSql("INSERT INTO map_doris_sink select * from map_dori_source");
    }
}
