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

import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Map;

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
    public void replaceDecimalTypeIfNeededTest1() throws Exception {
        ArrayList<Document> documents = new ArrayList<>();
        documents.add(new Document("fields1", 1234567.666666));
        documents.add(new Document("fields1", 123456789.88888888));

        MongoDBSchema mongoDBSchema = new MongoDBSchema(documents, "db_TEST", "test_table", "");
        Map<String, FieldSchema> fields = mongoDBSchema.getFields();
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            FieldSchema fieldSchema = entry.getValue();
            String fieldName = entry.getKey();
            if (fieldName.equals("fields1")) {
                assertEquals("DECIMAL(17,8)", fieldSchema.getTypeString());
            }
        }
    }

    @Test
    public void replaceDecimalTypeIfNeededTest2() throws Exception {
        ArrayList<Document> documents = new ArrayList<>();
        documents.add(new Document("fields1", 1234567.666666));
        documents.add(new Document("fields1", 123456789));

        MongoDBSchema mongoDBSchema = new MongoDBSchema(documents, "db_TEST", "test_table", "");
        Map<String, FieldSchema> fields = mongoDBSchema.getFields();
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            FieldSchema fieldSchema = entry.getValue();
            String fieldName = entry.getKey();
            if (fieldName.equals("fields1")) {
                assertEquals("DECIMAL(15,6)", fieldSchema.getTypeString());
            }
        }
    }

    @Test
    public void replaceDecimalTypeIfNeededTest3() throws Exception {
        ArrayList<Document> documents = new ArrayList<>();
        documents.add(new Document("fields1", 1234567.666666));
        documents.add(new Document("fields1", 123456789));
        documents.add(new Document("fields1", 1234567.7777777));
        documents.add(
                new Document("fields1", new Decimal128(new BigDecimal("12345679012.999999999"))));

        MongoDBSchema mongoDBSchema = new MongoDBSchema(documents, "db_TEST", "test_table", "");
        Map<String, FieldSchema> fields = mongoDBSchema.getFields();
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            FieldSchema fieldSchema = entry.getValue();
            String fieldName = entry.getKey();
            if (fieldName.equals("fields1")) {
                assertEquals("DECIMAL(20,9)", fieldSchema.getTypeString());
            }
        }
    }

    @Test
    public void replaceDecimalTypeIfNeededTest4() throws Exception {
        ArrayList<Document> documents = new ArrayList<>();
        documents.add(new Document("fields1", "yes"));
        documents.add(new Document("fields1", 1234567.666666));
        documents.add(new Document("fields1", 123456789));
        documents.add(new Document("fields1", 1234567.7777777));
        documents.add(
                new Document("fields1", new Decimal128(new BigDecimal("12345679012.999999999"))));

        MongoDBSchema mongoDBSchema = new MongoDBSchema(documents, "db_TEST", "test_table", "");
        Map<String, FieldSchema> fields = mongoDBSchema.getFields();
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            FieldSchema fieldSchema = entry.getValue();
            String fieldName = entry.getKey();
            if (fieldName.equals("fields1")) {
                assertEquals("STRING", fieldSchema.getTypeString());
            }
        }
    }

    @Test
    public void replaceDecimalTypeIfNeededTest5() throws Exception {
        ArrayList<Document> documents = new ArrayList<>();
        documents.add(new Document("fields1", 1234567.666666));
        documents.add(new Document("fields1", 123456789));
        documents.add(new Document("fields1", 1234567.7777777));
        documents.add(new Document("fields1", "yes"));
        documents.add(
                new Document("fields1", new Decimal128(new BigDecimal("12345679012.999999999"))));

        MongoDBSchema mongoDBSchema = new MongoDBSchema(documents, "db_TEST", "test_table", "");
        Map<String, FieldSchema> fields = mongoDBSchema.getFields();
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            FieldSchema fieldSchema = entry.getValue();
            String fieldName = entry.getKey();
            if (fieldName.equals("fields1")) {
                assertEquals("STRING", fieldSchema.getTypeString());
            }
        }
    }

    @Test
    public void replaceDecimalTypeIfNeededTest6() throws Exception {
        ArrayList<Document> documents = new ArrayList<>();
        documents.add(new Document("fields1", 1234567.666666));
        documents.add(new Document("fields1", 123456789));
        documents.add(new Document("fields1", 1234567.7777777));
        documents.add(new Document("fields1", 123444555433445L));
        documents.add(
                new Document("fields1", new Decimal128(new BigDecimal("12345679012.999999999"))));

        MongoDBSchema mongoDBSchema = new MongoDBSchema(documents, "db_TEST", "test_table", "");
        Map<String, FieldSchema> fields = mongoDBSchema.getFields();
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            FieldSchema fieldSchema = entry.getValue();
            String fieldName = entry.getKey();
            if (fieldName.equals("fields1")) {
                assertEquals("DECIMAL(24,9)", fieldSchema.getTypeString());
            }
        }
    }
}
