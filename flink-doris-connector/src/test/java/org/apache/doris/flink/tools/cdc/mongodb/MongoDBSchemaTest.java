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

import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class MongoDBSchemaTest {

    @Test
    public void convertToDorisType() {}

    @Test
    public void getCdcTableName() throws Exception {
        MongoDBSchema mongoDBSchema =
                new MongoDBSchema(new ArrayList<Document>(), "db_TEST", "test_table", "");
        assertEquals("db_TEST\\.test_table", mongoDBSchema.getCdcTableName());
    }

    @Test
    public void replaceDecimalTypeIfNeeded() throws Exception {
        ArrayList<Document> documents = new ArrayList<>();
        documents.add(new Document("fields1", 1234567.666666));
        MongoDBSchema mongoDBSchema = new MongoDBSchema(documents, "db_TEST", "test_table", "");
        String d = mongoDBSchema.replaceDecimalTypeIfNeeded("fields1", "DECIMALV3(12,8)");
        assertEquals("DECIMAL(15,8)", d);
    }
}
