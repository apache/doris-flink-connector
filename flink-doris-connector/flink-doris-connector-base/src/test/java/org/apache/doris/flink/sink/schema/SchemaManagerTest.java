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

import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.HttpEntityMock;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicStatusLine;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class SchemaManagerTest {

    static String queryResponse =
            "{\n"
                    + "    \"data\": {\n"
                    + "        \"type\": \"result_set\",\n"
                    + "        \"meta\": [{\"name\":\"COLUMN_NAME\",\"type\":\"CHAR\"}],\n"
                    + "        \"data\": [\n"
                    + "            [\"age\"]\n"
                    + "        ],\n"
                    + "        \"time\": 15\n"
                    + "    },\n"
                    + "    \"msg\": \"success\",\n"
                    + "    \"code\": 0\n"
                    + "}";

    static String queryNoExistsResponse =
            "{\n"
                    + "    \"data\": {\n"
                    + "        \"type\": \"result_set\",\n"
                    + "        \"meta\": [{\"name\":\"COLUMN_NAME\",\"type\":\"CHAR\"}],\n"
                    + "        \"data\": [],\n"
                    + "        \"time\": 0\n"
                    + "    },\n"
                    + "    \"msg\": \"success\",\n"
                    + "    \"code\": 0\n"
                    + "}";

    HttpEntityMock entityMock;
    SchemaChangeManager schemaChangeManager;
    private MockedStatic<HttpClients> httpClientMockedStatic;
    private MockedStatic<BackendUtil> backendUtilMockedStatic;

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
        when(httpClient.execute(any())).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(normalLine);
        when(httpResponse.getEntity()).thenReturn(entityMock);

        httpClientMockedStatic = mockStatic(HttpClients.class);
        httpClientMockedStatic.when(() -> HttpClients.createDefault()).thenReturn(httpClient);

        backendUtilMockedStatic = mockStatic(BackendUtil.class);
        backendUtilMockedStatic.when(() -> BackendUtil.tryHttpConnection(any())).thenReturn(true);
    }

    @Test
    public void testColumnExists() throws IOException, IllegalArgumentException {
        entityMock.setValue(queryResponse);
        boolean columnExists = schemaChangeManager.checkColumnExists("test", "test_flink", "age");
        Assert.assertEquals(true, columnExists);
    }

    @Test
    public void testColumnNotExists() throws IOException, IllegalArgumentException {
        entityMock.setValue(queryNoExistsResponse);
        boolean columnExists = schemaChangeManager.checkColumnExists("test", "test_flink", "age1");
        Assert.assertEquals(false, columnExists);
    }

    @Test
    public void testAddColumn() {
        FieldSchema field = new FieldSchema("col", "int", "comment \"'sdf'");
        String addColumnDDL = SchemaChangeHelper.buildAddColumnDDL("test.test_flink", field);
        Assert.assertEquals(
                "ALTER TABLE `test`.`test_flink` ADD COLUMN `col` int COMMENT 'comment \"\\'sdf\\''",
                addColumnDDL);

        field = new FieldSchema("col", "int", "10", "comment \"'sdf'");
        addColumnDDL = SchemaChangeHelper.buildAddColumnDDL("test.test_flink", field);
        Assert.assertEquals(
                "ALTER TABLE `test`.`test_flink` ADD COLUMN `col` int DEFAULT '10' COMMENT 'comment \"\\'sdf\\''",
                addColumnDDL);

        field = new FieldSchema("col", "int", "current_timestamp", "comment \"'sdf'");
        addColumnDDL = SchemaChangeHelper.buildAddColumnDDL("test.test_flink", field);
        Assert.assertEquals(
                "ALTER TABLE `test`.`test_flink` ADD COLUMN `col` int DEFAULT current_timestamp COMMENT 'comment \"\\'sdf\\''",
                addColumnDDL);

        field = new FieldSchema("col", "int", "current_timestamp", "中文注释");
        addColumnDDL = SchemaChangeHelper.buildAddColumnDDL("test.test_flink", field);
        Assert.assertEquals(
                "ALTER TABLE `test`.`test_flink` ADD COLUMN `col` int DEFAULT current_timestamp COMMENT '中文注释'",
                addColumnDDL);
    }

    @Test
    public void testDropColumn() {
        String dropColumnDDL = SchemaChangeHelper.buildDropColumnDDL("test.test_flink", "col");
        Assert.assertEquals("ALTER TABLE `test`.`test_flink` DROP COLUMN `col`", dropColumnDDL);
    }

    @Test
    public void testRenameColumn() {
        String renameColumnDDL =
                SchemaChangeHelper.buildRenameColumnDDL("test.test_flink", "col", "col_new");
        Assert.assertEquals(
                "ALTER TABLE `test`.`test_flink` RENAME COLUMN `col` `col_new`", renameColumnDDL);
    }

    @After
    public void after() {
        if (httpClientMockedStatic != null) {
            httpClientMockedStatic.close();
        }
        if (backendUtilMockedStatic != null) {
            backendUtilMockedStatic.close();
        }
    }
}
