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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.flink.exception.DorisException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class SchemaUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(SchemaUtilsTest.class);

    @Test
    public void convertToSchema() throws DorisException {
        Field field1 =
                new Field("field1", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Field field2 =
                new Field("field2", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        Schema arrowSchema = new Schema(Arrays.asList(field1, field2));
        String schemaStr =
                "{\"properties\":["
                        + "{\"type\":\"int\",\"name\":\"field1\",\"comment\":\"\"}"
                        + ",{\"type\":\"int\",\"name\":\"field2\",\"comment\":\"\"}"
                        + "], \"status\":200}";
        org.apache.doris.flink.rest.models.Schema schema =
                RestService.parseSchema(schemaStr, logger);

        org.apache.doris.flink.rest.models.Schema result =
                SchemaUtils.convertToSchema(schema, arrowSchema);

        assertEquals(2, result.getProperties().size());
        assertEquals("field1", result.getProperties().get(0).getName());
        assertEquals("field2", result.getProperties().get(1).getName());
    }
}
