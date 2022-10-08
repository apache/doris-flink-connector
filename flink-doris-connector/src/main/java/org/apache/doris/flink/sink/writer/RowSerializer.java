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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.deserialization.converter.DorisRowConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import static org.apache.doris.flink.sink.writer.LoadConstants.CSV;
import static org.apache.doris.flink.sink.writer.LoadConstants.DORIS_DELETE_SIGN;
import static org.apache.doris.flink.sink.writer.LoadConstants.JSON;
import static org.apache.doris.flink.sink.writer.LoadConstants.NULL_VALUE;

/**
 * Serializer for {@link Row}.
 */
public class RowSerializer implements DorisRecordSerializer<Row> {
    String[] fieldNames;
    String type;
    private ObjectMapper objectMapper;
    private final String fieldDelimiter;
    private final boolean enableDelete;
    private final DorisRowConverter rowConverter;
    private final DataType[] dataTypes;

    private RowSerializer(String[] fieldNames, DataType[] dataTypes, String type, String fieldDelimiter, boolean enableDelete) {
        this.fieldNames = fieldNames;
        this.dataTypes = dataTypes;
        this.type = type;
        this.fieldDelimiter = fieldDelimiter;
        this.enableDelete = enableDelete;
        if (JSON.equals(type)) {
            objectMapper = new ObjectMapper();
        }
        this.rowConverter = new DorisRowConverter(dataTypes);
    }

    @Override
    public byte[] serialize(Row record) throws IOException{
        RowData rowDataRecord = RowRowConverter.create(DataTypes.ROW(dataTypes)).toInternal(record);
        int maxIndex = Math.min(record.getArity(), fieldNames.length);
        String valString;
        if (JSON.equals(type)) {
            valString = buildJsonString(rowDataRecord, maxIndex);
        } else if (CSV.equals(type)) {
            valString = buildCSVString(rowDataRecord, maxIndex);
        } else {
            throw new IllegalArgumentException("The type " + type + " is not supported!");
        }
        return valString.getBytes(StandardCharsets.UTF_8);
    }

    public String buildJsonString(RowData record, int maxIndex) throws IOException {
        int fieldIndex = 0;
        Map<String, String> valueMap = new HashMap<>();
        while (fieldIndex < maxIndex) {
            Object field = rowConverter.convertExternal(record, fieldIndex);
            String value = field != null ? field.toString() : null;
            valueMap.put(fieldNames[fieldIndex], value);
            fieldIndex++;
        }
        if (enableDelete) {
            valueMap.put(DORIS_DELETE_SIGN, parseDeleteSign(record.getRowKind()));
        }
        return objectMapper.writeValueAsString(valueMap);
    }

    public String buildCSVString(RowData record, int maxIndex) throws IOException {
        int fieldIndex = 0;
        StringJoiner joiner = new StringJoiner(fieldDelimiter);
        while (fieldIndex < maxIndex) {
            Object field = rowConverter.convertExternal(record, fieldIndex);
            String value = field != null ? field.toString() : NULL_VALUE;
            joiner.add(value);
            fieldIndex++;
        }
        if (enableDelete) {
            joiner.add(parseDeleteSign(record.getRowKind()));
        }
        return joiner.toString();
    }

    public String parseDeleteSign(RowKind rowKind) {
        if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_AFTER.equals(rowKind)) {
            return "0";
        } else if (RowKind.DELETE.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
            return "1";
        } else {
            throw new IllegalArgumentException("Unrecognized row kind:" + rowKind.toString());
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for RowSerializer.
     */
    public static class Builder {
        private String[] fieldNames;
        private DataType[] dataTypes;
        private String type;
        private String fieldDelimiter;
        private boolean deletable;

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldType(DataType[] dataTypes) {
            this.dataTypes = dataTypes;
            return this;
        }

        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        public Builder setFieldDelimiter(String fieldDelimiter) {
            this.fieldDelimiter = fieldDelimiter;
            return this;
        }

        public Builder enableDelete(boolean deletable) {
            this.deletable = deletable;
            return this;
        }

        public RowSerializer build() {
            Preconditions.checkState(CSV.equals(type) && fieldDelimiter != null || JSON.equals(type));
            Preconditions.checkNotNull(dataTypes);
            Preconditions.checkNotNull(fieldNames);
            return new RowSerializer(fieldNames, dataTypes, type, fieldDelimiter, deletable);
        }
    }
}
