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
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.http.client.methods.HttpPost;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

public class SchemaChangeManagerTest {
    @Test
    public void testBuildEncodedSchemaChangeUrls() throws Exception {
        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("ods.ods_新券表_copy1")
                        .setUsername("root")
                        .setPassword("")
                        .build();
        try (MockedStatic<BackendUtil> backendUtilMockedStatic = mockStatic(BackendUtil.class)) {
            backendUtilMockedStatic
                    .when(() -> BackendUtil.tryHttpConnection(any()))
                    .thenReturn(true);
            SchemaChangeManager manager = new SchemaChangeManager(dorisOptions);

            Assert.assertEquals(
                    "http://127.0.0.1:8030/api/enable_light_schema_change/ods/ods_%E6%96%B0%E5%88%B8%E8%A1%A8_copy1",
                    manager.buildCheckSchemaChangeUrl("ods", "ods_新券表_copy1"));
            Assert.assertEquals(
                    "http://127.0.0.1:8030/api/query/default_cluster/%E6%95%B0%E4%BB%93",
                    manager.buildSchemaChangeUrl("数仓"));

            HttpPost httpPost = manager.buildHttpPost("select 1", "数仓");
            Assert.assertEquals(
                    "http://127.0.0.1:8030/api/query/default_cluster/%E6%95%B0%E4%BB%93",
                    httpPost.getURI().toASCIIString());
        }
    }
}
