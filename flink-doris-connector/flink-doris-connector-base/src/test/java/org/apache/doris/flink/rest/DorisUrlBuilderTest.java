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

package org.apache.doris.flink.rest;

import org.junit.Assert;
import org.junit.Test;

public class DorisUrlBuilderTest {

    @Test
    public void testHttpUrls() {
        DorisUrlBuilder builder = new DorisUrlBuilder(false);
        Assert.assertEquals(
                "http://host:8030/api/db/tbl/_stream_load",
                builder.streamLoad("host:8030", "db", "tbl"));
        Assert.assertEquals(
                "http://host:8030/api/db/_stream_load_2pc",
                builder.streamLoad2pc("host:8030", "db"));
        Assert.assertEquals(
                "http://host:8030/api/db/tbl/_schema",
                builder.tableSchema("host:8030", "db", "tbl"));
        Assert.assertEquals(
                "http://host:8030/api/catalog/db/tbl/_schema",
                builder.catalogTableSchema("host:8030", "catalog", "db", "tbl"));
        Assert.assertEquals(
                "http://host:8030/api/db/tbl/_query_plan",
                builder.queryPlan("host:8030", "db", "tbl"));
        Assert.assertEquals(
                "http://host:8030/api/query/default_cluster/information_schema",
                builder.informationSchemaQuery("host:8030"));
        Assert.assertEquals(
                "http://host:8030/api/backends?is_alive=true", builder.backends("host:8030"));
        Assert.assertEquals("http://host:8030/copy/upload", builder.copyUpload("host:8030"));
        Assert.assertEquals("http://host:8030/copy/query", builder.copyQuery("host:8030"));
    }

    @Test
    public void testHttpsUrls() {
        DorisUrlBuilder builder = new DorisUrlBuilder(true);
        Assert.assertEquals(
                "https://host:8030/api/db/tbl/_stream_load",
                builder.streamLoad("host:8030", "db", "tbl"));
        Assert.assertEquals(
                "https://host:8030/api/db/_stream_load_2pc",
                builder.streamLoad2pc("host:8030", "db"));
        Assert.assertEquals(
                "https://host:8030/api/db/tbl/_schema",
                builder.tableSchema("host:8030", "db", "tbl"));
        Assert.assertEquals(
                "https://host:8030/api/catalog/db/tbl/_schema",
                builder.catalogTableSchema("host:8030", "catalog", "db", "tbl"));
        Assert.assertEquals(
                "https://host:8030/api/db/tbl/_query_plan",
                builder.queryPlan("host:8030", "db", "tbl"));
        Assert.assertEquals(
                "https://host:8030/api/query/default_cluster/information_schema",
                builder.informationSchemaQuery("host:8030"));
        Assert.assertEquals(
                "https://host:8030/api/backends?is_alive=true", builder.backends("host:8030"));
        Assert.assertEquals("https://host:8030/copy/upload", builder.copyUpload("host:8030"));
        Assert.assertEquals("https://host:8030/copy/query", builder.copyQuery("host:8030"));
    }
}
