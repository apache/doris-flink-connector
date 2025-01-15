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

import org.apache.flink.api.java.tuple.Tuple2;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.doris.flink.catalog.doris.DorisType;
import org.bson.BsonArray;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MongoDBTypeTest {

    @Test
    public void toDorisType() {
        assertEquals(DorisType.INT, MongoDBType.toDorisType(new Integer(123)));
        assertEquals(DorisType.DATETIME_V2 + "(3)", MongoDBType.toDorisType(new Date()));
        assertEquals(DorisType.DATETIME_V2 + "(0)", MongoDBType.toDorisType(new BsonTimestamp()));
        assertEquals(DorisType.BIGINT, MongoDBType.toDorisType(new Long(1234567891)));
        assertEquals("DECIMALV3(6,2)", MongoDBType.toDorisType(new Double("1234.56")));
        assertEquals(DorisType.BOOLEAN, MongoDBType.toDorisType(new Boolean(true)));
        assertEquals(DorisType.STRING, MongoDBType.toDorisType("string"));
        assertEquals(
                DorisType.VARCHAR + "(30)",
                MongoDBType.toDorisType(new ObjectId("66583533791a67a6f8c5a339")));
        assertEquals(
                "DECIMALV3(10,5)",
                MongoDBType.toDorisType(new Decimal128(new BigDecimal("12345.55555"))));
        BsonArray bsonArray = new BsonArray();
        bsonArray.add(new BsonString("string"));
        bsonArray.add(new BsonInt64(123456789));
        assertEquals(DorisType.ARRAY + "<STRING>", MongoDBType.toDorisType(bsonArray));
    }

    @Test
    public void jsonNodeToDorisType() {
        assertEquals(DorisType.INT, MongoDBType.jsonNodeToDorisType(new IntNode(1234)));
        assertEquals(DorisType.STRING, MongoDBType.jsonNodeToDorisType(new TextNode("text")));
        assertEquals(DorisType.BIGINT, MongoDBType.jsonNodeToDorisType(new LongNode(1234568948)));
        assertEquals(DorisType.DOUBLE, MongoDBType.jsonNodeToDorisType(new DoubleNode(1234.23)));
        assertEquals(DorisType.BOOLEAN, MongoDBType.jsonNodeToDorisType(BooleanNode.TRUE));
        assertEquals(
                DorisType.ARRAY + "<STRING>",
                MongoDBType.jsonNodeToDorisType(JsonNodeFactory.instance.arrayNode()));
        assertEquals(
                "DECIMALV3(6,2)",
                MongoDBType.jsonNodeToDorisType(new DecimalNode(new BigDecimal("1234.23"))));

        ObjectNode dateJsonNodes = JsonNodeFactory.instance.objectNode();
        dateJsonNodes.put(ChangeStreamConstant.DATE_FIELD, "");
        assertEquals(DorisType.DATETIME_V2 + "(3)", MongoDBType.jsonNodeToDorisType(dateJsonNodes));

        ObjectNode timestampJsonNodes = JsonNodeFactory.instance.objectNode();
        timestampJsonNodes.put(ChangeStreamConstant.TIMESTAMP_FIELD, "");
        assertEquals(
                DorisType.DATETIME_V2 + "(0)", MongoDBType.jsonNodeToDorisType(timestampJsonNodes));

        ObjectNode decimalJsonNodes = JsonNodeFactory.instance.objectNode();
        decimalJsonNodes.put(ChangeStreamConstant.DECIMAL_FIELD, "1234.23");
        assertEquals("DECIMALV3(6,2)", MongoDBType.jsonNodeToDorisType(decimalJsonNodes));

        ObjectNode longJsonNodes = JsonNodeFactory.instance.objectNode();
        longJsonNodes.put(ChangeStreamConstant.LONG_FIELD, "1234234466");
        assertEquals(DorisType.BIGINT, MongoDBType.jsonNodeToDorisType(longJsonNodes));
    }

    @Test
    public void getDecimalPrecisionAndScale() {
        String decimalString1 = "DECIMAL(13,6)";
        String decimalString2 = "DECIMAL(20,10)";
        String decimalString3 = "DECIMAL(5,10)";
        String decimalString4 = "DECIMAL(10,5)";

        Tuple2<Integer, Integer> decimalPrecision1 =
                MongoDBType.getDecimalPrecisionAndScale(decimalString1);
        assertArrayEquals(
                new int[] {13, 6}, new int[] {decimalPrecision1.f0, decimalPrecision1.f1});

        Tuple2<Integer, Integer> decimalPrecision2 =
                MongoDBType.getDecimalPrecisionAndScale(decimalString2);
        assertArrayEquals(
                new int[] {20, 10}, new int[] {decimalPrecision2.f0, decimalPrecision2.f1});

        Tuple2<Integer, Integer> decimalPrecision3 =
                MongoDBType.getDecimalPrecisionAndScale(decimalString3);
        assertArrayEquals(
                new int[] {5, 10}, new int[] {decimalPrecision3.f0, decimalPrecision3.f1});

        Tuple2<Integer, Integer> decimalPrecision4 =
                MongoDBType.getDecimalPrecisionAndScale(decimalString4);
        assertArrayEquals(
                new int[] {10, 5}, new int[] {decimalPrecision4.f0, decimalPrecision4.f1});
    }

    @Test
    public void checkAndRebuildBigDecimal() {

        HashMap<String, String> decimalTestMap =
                new HashMap<String, String>() {
                    {
                        put("123456789.55555", "DECIMALV3(14,5)");
                        put("123456789.666666", "DECIMALV3(15,6)");
                        put("123456789.7777777", "DECIMALV3(16,7)");
                        put("123456789.88888888", "DECIMALV3(17,8)");
                        put("123456789.999999999", "DECIMALV3(18,9)");
                        put("123456789.1", "DECIMALV3(10,1)");
                        put("123456789.22", "DECIMALV3(11,2)");
                        put("12345E4", "DECIMALV3(9,0)");
                        put("0.12345E4", "DECIMALV3(5,1)");
                    }
                };

        decimalTestMap.forEach(
                (k, v) ->
                        assertEquals(MongoDBType.checkAndRebuildBigDecimal(new BigDecimal(k)), v));
    }
}
