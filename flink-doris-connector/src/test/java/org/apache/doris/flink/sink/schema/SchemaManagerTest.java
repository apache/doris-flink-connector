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

package org.apache.doris.flink.sink.schema;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.HttpEntityMock;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class SchemaManagerTest {

    static String QUERY_RESPONSE = "{\n" +
            "    \"data\": {\n" +
            "        \"type\": \"result_set\",\n" +
            "        \"meta\": [{\"name\":\"COLUMN_NAME\",\"type\":\"CHAR\"}],\n" +
            "        \"data\": [\n" +
            "            [\"age\"]\n" +
            "        ],\n" +
            "        \"time\": 15\n" +
            "    },\n" +
            "    \"msg\": \"success\",\n" +
            "    \"code\": 0\n" +
            "}";

    static String QUERY_NO_EXISTS_RESPONSE = "{\n" +
            "    \"data\": {\n" +
            "        \"type\": \"result_set\",\n" +
            "        \"meta\": [{\"name\":\"COLUMN_NAME\",\"type\":\"CHAR\"}],\n" +
            "        \"data\": [],\n" +
            "        \"time\": 0\n" +
            "    },\n" +
            "    \"msg\": \"success\",\n" +
            "    \"code\": 0\n" +
            "}";

    HttpEntityMock entityMock;
    SchemaChangeManager schemaChangeManager;

    @Before
    public void setUp() throws IOException {
        DorisOptions dorisOptions = OptionUtils.buildDorisOptions();
        schemaChangeManager = new SchemaChangeManager(dorisOptions);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        entityMock = new HttpEntityMock();

        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        StatusLine normalLine = new BasicStatusLine(new ProtocolVersion("http", 1, 0), 200, "");

        when(httpClient.execute(any())).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(normalLine);
        when(httpResponse.getEntity()).thenReturn(entityMock);

        MockedStatic<HttpClients> httpClientMockedStatic = mockStatic(HttpClients.class);

        when(httpClient.execute(any())).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(normalLine);
        when(httpResponse.getEntity()).thenReturn(entityMock);

        httpClientMockedStatic.when(()-> HttpClients.createDefault())
                .thenReturn(httpClient);
    }

    @Test
    public void testColumnExists() throws IOException, IllegalArgumentException {
        entityMock.setValue(QUERY_RESPONSE);
        boolean columnExists = schemaChangeManager.checkColumnExists("test", "test_flink", "age");
        System.out.println(columnExists);
    }

    @Test
    public void testColumnNotExists() throws IOException, IllegalArgumentException {
        entityMock.setValue(QUERY_NO_EXISTS_RESPONSE);
        boolean columnExists = schemaChangeManager.checkColumnExists("test", "test_flink", "age1");
        System.out.println(columnExists);
    }

}
