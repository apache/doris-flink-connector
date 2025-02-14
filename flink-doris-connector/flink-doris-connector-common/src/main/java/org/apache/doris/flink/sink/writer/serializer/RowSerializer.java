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

package org.apache.doris.flink.sink.writer.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

import static org.apache.doris.flink.sink.writer.LoadConstants.CSV;
import static org.apache.doris.flink.sink.writer.LoadConstants.JSON;

/**
 * Serializer for {@link Row}. Quick way to support RowSerializer on existing code.
 *
 * <p>TODO: support original Doris to Row serializer
 */
@PublicEvolving
public class RowSerializer implements DorisRecordSerializer<Row> {
    /** converter {@link Row} to {@link RowData}. */
    private final RowRowConverter rowRowConverter;

    private final RowDataSerializer rowDataSerializer;

    private RowSerializer(
            String[] fieldNames,
            DataType[] dataTypes,
            String type,
            String fieldDelimiter,
            boolean enableDelete) {
        this.rowRowConverter = RowRowConverter.create(DataTypes.ROW(dataTypes));
        this.rowDataSerializer =
                RowDataSerializer.builder()
                        .setFieldNames(fieldNames)
                        .setFieldType(dataTypes)
                        .setType(type)
                        .setFieldDelimiter(fieldDelimiter)
                        .enableDelete(enableDelete)
                        .build();
    }

    @Override
    public DorisRecord serialize(Row record) throws IOException {
        RowData rowDataRecord = this.rowRowConverter.toInternal(record);
        return this.rowDataSerializer.serialize(rowDataRecord);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for RowSerializer. */
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
            Preconditions.checkState(
                    CSV.equals(type) && fieldDelimiter != null || JSON.equals(type));
            Preconditions.checkNotNull(dataTypes);
            Preconditions.checkNotNull(fieldNames);
            return new RowSerializer(fieldNames, dataTypes, type, fieldDelimiter, deletable);
        }
    }
}
