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

package org.apache.doris.flink.sink.writer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisOptions;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * test for JsonDebeziumSchemaSerializer.
 */
public class TestJsonDebeziumSchemaSerializer {

    static DorisOptions dorisOptions;
    static JsonDebeziumSchemaSerializer serializer;
    static ObjectMapper objectMapper = new ObjectMapper();

    @BeforeClass
    public static void setUp() {
        dorisOptions = DorisOptions.builder().setFenodes("161.189.169.254:8030")
                .setTableIdentifier("test.test")
                .setUsername("root")
                .setPassword("").build();
        serializer = JsonDebeziumSchemaSerializer.builder().build();
    }

    @Test
    public void testSerializeInsert() throws IOException {
        byte[] serializedValue = serializer.serialize("{\"before\":null,\"after\":{\"id\":1,\"name\":\"zhangsan\",\"timestamp\":\"2022-06-16 15:53:40\",\"datetime\":\"2022-06-16 15:53:46\",\"date\":\"2022-06-16\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1655306397000,\"snapshot\":\"false\",\"db\":\"test\",\"sequence\":null,\"table\":\"test\",\"server_id\":1,\"gtid\":null,\"file\":\"mysql-bin.000001\",\"pos\":972,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1655306397334,\"transaction\":null}");
        Map<String, String> valueMap = objectMapper.readValue(new String(serializedValue, StandardCharsets.UTF_8), new TypeReference<Map<String, String>>(){});
        Assert.assertEquals("1", valueMap.get("id"));
        Assert.assertEquals("zhangsan", valueMap.get("name"));
        Assert.assertEquals("2022-06-16 15:53:40", valueMap.get("timestamp"));
        Assert.assertEquals("2022-06-16 15:53:46", valueMap.get("datetime"));
        Assert.assertEquals("2022-06-16", valueMap.get("date"));
    }

    @Test
    public void testSerializeUpdate() throws IOException {
        byte[] serializedValue = serializer.serialize("");
        Assert.assertArrayEquals("1|zhangsan|".getBytes(StandardCharsets.UTF_8), serializedValue);
    }

    @Test
    public void testSerializeDelete() throws IOException {
        byte[] serializedValue = serializer.serialize("");
        System.out.println(new String(serializedValue));
        Assert.assertArrayEquals("1|zhangsan|".getBytes(StandardCharsets.UTF_8), serializedValue);
    }

    @Test
    public void testSerializeAddColumn() throws IOException {
        byte[] serializedValue = serializer.serialize("");
        System.out.println(new String(serializedValue));
        Assert.assertArrayEquals("1|zhangsan|".getBytes(StandardCharsets.UTF_8), serializedValue);
    }

    @Test
    public void testSerializeDeleteColumn() throws IOException {
        byte[] serializedValue = serializer.serialize("");
        System.out.println(new String(serializedValue));
        Assert.assertArrayEquals("1|zhangsan|".getBytes(StandardCharsets.UTF_8), serializedValue);
    }

    @Test
    public void testSerializeTruncate() throws IOException {
        byte[] serializedValue = serializer.serialize("");
        System.out.println(new String(serializedValue));
        Assert.assertArrayEquals("1|zhangsan|".getBytes(StandardCharsets.UTF_8), serializedValue);
    }
}
