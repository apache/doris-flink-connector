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

package org.apache.doris.flink.tools.cdc.db2;

import org.apache.doris.flink.catalog.doris.DorisType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Db2TypeTest {
    @Test
    public void db2FullTypeTest() {
        assertEquals(DorisType.BOOLEAN, Db2Type.toDorisType("BOOLEAN", 1, null));
        assertEquals(DorisType.SMALLINT, Db2Type.toDorisType("SMALLINT", 5, 0));
        assertEquals(DorisType.INT, Db2Type.toDorisType("INTEGER", 10, 0));
        assertEquals(DorisType.BIGINT, Db2Type.toDorisType("BIGINT", 10, 0));
        assertEquals(DorisType.FLOAT, Db2Type.toDorisType("REAL", 24, null));
        assertEquals(DorisType.DOUBLE, Db2Type.toDorisType("DOUBLE", 53, null));
        assertEquals("DECIMALV3(34,0)", Db2Type.toDorisType("DECFLOAT", 34, null));
        assertEquals("DECIMALV3(31,0)", Db2Type.toDorisType("DECIMAL", 31, 0));
        assertEquals("DECIMALV3(31,31)", Db2Type.toDorisType("DECIMAL", 31, 31));
        assertEquals("DECIMALV3(31,0)", Db2Type.toDorisType("NUMERIC", 31, 0));
        assertEquals("DECIMALV3(31,31)", Db2Type.toDorisType("NUMERIC", 31, 31));
        assertEquals("VARCHAR(600)", Db2Type.toDorisType("VARCHAR", 200, null));
        assertEquals(DorisType.STRING, Db2Type.toDorisType("VARCHAR", 32672, null));
        assertEquals(DorisType.VARCHAR + "(3)", Db2Type.toDorisType("CHAR", 1, null));
        assertEquals(DorisType.VARCHAR + "(765)", Db2Type.toDorisType("CHAR", 255, null));
        assertEquals(DorisType.DATETIME_V2 + "(0)", Db2Type.toDorisType("TIMESTAMP", 26, 0));
        assertEquals(DorisType.DATETIME_V2 + "(6)", Db2Type.toDorisType("TIMESTAMP", 26, 6));
        assertEquals(DorisType.DATETIME_V2 + "(6)", Db2Type.toDorisType("TIMESTAMP", 26, 9));
        assertEquals(DorisType.DATE_V2, Db2Type.toDorisType("DATE", 10, null));
        assertEquals(DorisType.STRING, Db2Type.toDorisType("TIME", 8, 0));
    }
}
