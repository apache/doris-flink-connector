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

package org.apache.doris.flink.tools.cdc.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MongoParsingProcessFunctionTest {

    @Test
    public void getRecordTableName() throws Exception {
        String record =
                "{\"_id\":\"{\\\"_id\\\": {\\\"$oid\\\": \\\"66583533791a67a6f8c5a339\\\"}}\",\"operationType\":\"insert\",\"fullDocument\":\"{\\\"_id\\\": {\\\"$oid\\\": \\\"66583533791a67a6f8c5a339\\\"}, \\\"key1\\\": \\\"value1\\\"}\",\"source\":{\"ts_ms\":0,\"snapshot\":\"true\"},\"ts_ms\":1717065582062,\"ns\":{\"db\":\"test\",\"coll\":\"cdc_test\"},\"to\":null,\"documentKey\":\"{\\\"_id\\\": {\\\"$oid\\\": \\\"66583533791a67a6f8c5a339\\\"}}\",\"updateDescription\":null,\"clusterTime\":null,\"txnNumber\":null,\"lsid\":null}";
        MongoParsingProcessFunction mongoParsingProcessFunction =
                new MongoParsingProcessFunction(null, null);
        String recordTableName = mongoParsingProcessFunction.getRecordTableName(record);
        assertEquals("cdc_test", recordTableName);
    }
}
