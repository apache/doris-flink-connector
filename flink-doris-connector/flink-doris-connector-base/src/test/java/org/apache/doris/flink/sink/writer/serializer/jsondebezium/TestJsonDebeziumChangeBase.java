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

package org.apache.doris.flink.sink.writer.serializer.jsondebezium;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.writer.LoadConstants;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

/** Test base for data change and schema change. */
public class TestJsonDebeziumChangeBase {

    protected static DorisOptions dorisOptions;
    protected Map<String, String> tableMapping = new HashMap<>();
    protected boolean ignoreUpdateBefore = true;
    protected String lineDelimiter = LoadConstants.LINE_DELIMITER_DEFAULT;
    protected ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() {
        dorisOptions =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("test.t1")
                        .setUsername("root")
                        .setPassword("")
                        .build();
        this.tableMapping.put("mysql.t1", "doris.t1");
        this.tableMapping.put("mysql.t2", "doris.t2");
        this.tableMapping.put("test.dbo.t1", "test.t1");
        this.objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
        this.objectMapper.setNodeFactory(jsonNodeFactory);
    }
}
