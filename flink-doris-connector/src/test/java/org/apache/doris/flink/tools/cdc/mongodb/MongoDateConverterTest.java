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

public class MongoDateConverterTest {

    @Test
    public void convertTimestampToString() {
        Long timestamp = 1717488217456L;
        String formatStr = MongoDateConverter.convertTimestampToString(timestamp);
        assertEquals("2024-06-04 16:03:37.456000", formatStr);
    }
}
