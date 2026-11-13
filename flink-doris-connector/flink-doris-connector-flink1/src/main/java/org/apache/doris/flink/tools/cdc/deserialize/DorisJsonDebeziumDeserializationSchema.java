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

package org.apache.doris.flink.tools.cdc.deserialize;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.ConnectSchema;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.doris.flink.exception.DorisException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;

/** Currently just use for synchronous mysql non-default. */
public class DorisJsonDebeziumDeserializationSchema
        implements DebeziumDeserializationSchema<String> {

    private static final JsonNodeFactory JSON_NODE_FACTORY =
            JsonNodeFactory.withExactBigDecimals(true);
    private final ObjectMapper objectMapper;

    public DorisJsonDebeziumDeserializationSchema() {
        objectMapper = new ObjectMapper();
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector)
            throws Exception {
        Schema schema = sourceRecord.valueSchema();
        Object value = sourceRecord.value();
        JsonNode jsonValue = convertToJson(schema, value);
        byte[] bytes = objectMapper.writeValueAsString(jsonValue).getBytes(StandardCharsets.UTF_8);
        collector.collect(new String(bytes));
    }

    private JsonNode convertToJson(Schema schema, Object value) throws DorisException {
        if (value == null) {
            // Any schema is valid and we don't have a default, so treat this as an optional schema
            if (schema == null) {
                return null;
            }
            if (schema.isOptional()) {
                return JSON_NODE_FACTORY.nullNode();
            }
            throw new DorisException(
                    "Conversion error: null value for field that is required and has no default value");
        }

        try {
            final Schema.Type schemaType;
            if (schema == null) {
                schemaType = ConnectSchema.schemaType(value.getClass());
                if (schemaType == null) {
                    throw new DorisException(
                            "Java class "
                                    + value.getClass()
                                    + " does not have corresponding schema type.");
                }
            } else {
                schemaType = schema.type();
            }
            switch (schemaType) {
                case INT8:
                    return JSON_NODE_FACTORY.numberNode((Byte) value);
                case INT16:
                    return JSON_NODE_FACTORY.numberNode((Short) value);
                case INT32:
                    return JSON_NODE_FACTORY.numberNode((Integer) value);
                case INT64:
                    return JSON_NODE_FACTORY.numberNode((Long) value);
                case FLOAT32:
                    return JSON_NODE_FACTORY.numberNode((Float) value);
                case FLOAT64:
                    return JSON_NODE_FACTORY.numberNode((Double) value);
                case BOOLEAN:
                    return JSON_NODE_FACTORY.booleanNode((Boolean) value);
                case STRING:
                    CharSequence charSeq = (CharSequence) value;
                    return JSON_NODE_FACTORY.textNode(charSeq.toString());
                case BYTES:
                    if (value instanceof byte[]) {
                        return JSON_NODE_FACTORY.binaryNode((byte[]) value);
                    } else if (value instanceof ByteBuffer) {
                        return JSON_NODE_FACTORY.binaryNode(((ByteBuffer) value).array());
                    } else if (value instanceof BigDecimal) {
                        return JSON_NODE_FACTORY.numberNode((BigDecimal) value);
                    } else {
                        throw new DorisException(
                                "Invalid type for bytes type: " + value.getClass());
                    }
                case ARRAY:
                    {
                        Collection<?> collection = (Collection<?>) value;
                        ArrayNode list = JSON_NODE_FACTORY.arrayNode();
                        for (Object elem : collection) {
                            Schema valueSchema = schema == null ? null : schema.valueSchema();
                            JsonNode fieldValue = convertToJson(valueSchema, elem);
                            list.add(fieldValue);
                        }
                        return list;
                    }
                case MAP:
                    {
                        Map<?, ?> map = (Map<?, ?>) value;
                        // If true, using string keys and JSON object; if false, using non-string
                        // keys and Array-encoding
                        boolean objectMode;
                        if (schema == null) {
                            objectMode = true;
                            for (Map.Entry<?, ?> entry : map.entrySet()) {
                                if (!(entry.getKey() instanceof String)) {
                                    objectMode = false;
                                    break;
                                }
                            }
                        } else {
                            objectMode = schema.keySchema().type() == Schema.Type.STRING;
                        }
                        ObjectNode obj = null;
                        ArrayNode list = null;
                        if (objectMode) {
                            obj = JSON_NODE_FACTORY.objectNode();
                        } else {
                            list = JSON_NODE_FACTORY.arrayNode();
                        }
                        for (Map.Entry<?, ?> entry : map.entrySet()) {
                            Schema keySchema = schema == null ? null : schema.keySchema();
                            Schema valueSchema = schema == null ? null : schema.valueSchema();
                            JsonNode mapKey = convertToJson(keySchema, entry.getKey());
                            JsonNode mapValue = convertToJson(valueSchema, entry.getValue());

                            if (objectMode) {
                                obj.set(mapKey.asText(), mapValue);
                            } else {
                                list.add(JSON_NODE_FACTORY.arrayNode().add(mapKey).add(mapValue));
                            }
                        }
                        return objectMode ? obj : list;
                    }
                case STRUCT:
                    {
                        Struct struct = (Struct) value;
                        if (!struct.schema().equals(schema)) {
                            throw new DorisException("Mismatching schema.");
                        }
                        ObjectNode obj = JSON_NODE_FACTORY.objectNode();
                        for (Field field : schema.fields()) {
                            obj.set(
                                    field.name(),
                                    convertToJson(
                                            field.schema(),
                                            struct.getWithoutDefault(field.name())));
                        }
                        return obj;
                    }
            }
            throw new DorisException("Couldn't convert " + value + " to JSON.");
        } catch (ClassCastException e) {
            String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new DorisException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
        }
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
