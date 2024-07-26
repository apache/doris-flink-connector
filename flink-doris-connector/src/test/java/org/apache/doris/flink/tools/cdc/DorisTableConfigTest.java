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

package org.apache.doris.flink.tools.cdc;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DorisTableConfigTest {

    private DorisTableConfig dorisTableConfig;

    @Before
    public void init() {
        dorisTableConfig = new DorisTableConfig();
    }

    @Test
    public void buildTableBucketMapTest() {
        String tableBuckets = "tbl1:10,tbl2 : 20, a.* :30,b.*:40,.*:50";
        Map<String, Integer> tableBucketsMap = dorisTableConfig.buildTableBucketMap(tableBuckets);
        assertEquals(10, tableBucketsMap.get("tbl1").intValue());
        assertEquals(20, tableBucketsMap.get("tbl2").intValue());
        assertEquals(30, tableBucketsMap.get("a.*").intValue());
        assertEquals(40, tableBucketsMap.get("b.*").intValue());
        assertEquals(50, tableBucketsMap.get(".*").intValue());
    }
}
