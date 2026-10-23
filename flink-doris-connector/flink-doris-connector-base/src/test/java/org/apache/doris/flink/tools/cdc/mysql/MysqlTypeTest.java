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

package org.apache.doris.flink.tools.cdc.mysql;

import org.apache.doris.flink.catalog.doris.DorisType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MysqlTypeTest {

    @Test
    public void varcharTypeTest() {
        // utf8mb4 characters take up to 4 bytes each in UTF-8
        assertEquals("VARCHAR(4)", MysqlType.toDorisType("VARCHAR", 1, null));
        assertEquals("VARCHAR(400)", MysqlType.toDorisType("VARCHAR", 100, null));
        assertEquals("VARCHAR(1020)", MysqlType.toDorisType("VARCHAR", 255, null));
        assertEquals("VARCHAR(400)", MysqlType.toDorisType("CHAR", 100, null));
        // 16383 * 4 = 65532 <= 65533, still fits in VARCHAR
        assertEquals("VARCHAR(65532)", MysqlType.toDorisType("VARCHAR", 16383, null));
        // 16384 * 4 = 65536 > 65533, fall back to STRING
        assertEquals(DorisType.STRING, MysqlType.toDorisType("VARCHAR", 16384, null));
    }
}
