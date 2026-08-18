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

package org.apache.doris.flink.sink.writer.tvf;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.doris.flink.sink.writer.LoadConstants.JSON;

/** Serializes the fixed TVF column set as one JSON object per row. */
public class S3TvfRowDataSerializer extends RowDataSerializer {

    private static final long serialVersionUID = 1L;
    // S3 TVF lowercases inferred column names, while JSON key matching is case-sensitive.
    private static final String TVF_DELETE_SIGN = "__doris_delete_sign__";

    private final List<String> selectedColumns;
    private final int[] selectedIndexes;
    private final boolean deletable;

    public S3TvfRowDataSerializer(
            String[] fieldNames,
            DataType[] dataTypes,
            List<String> selectedColumns,
            boolean deletable) {
        super(fieldNames, dataTypes, JSON, null, deletable);
        Preconditions.checkArgument(fieldNames.length == dataTypes.length);
        Preconditions.checkArgument(!selectedColumns.isEmpty());
        this.selectedColumns = Collections.unmodifiableList(new ArrayList<>(selectedColumns));
        this.selectedIndexes = resolveIndexes(fieldNames, selectedColumns);
        this.deletable = deletable;
    }

    public boolean isDeleteSignEnabled() {
        return deletable;
    }

    public List<String> getSelectedColumns() {
        return selectedColumns;
    }

    @Override
    public String buildJsonString(RowData record, int maxIndex) throws IOException {
        Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < selectedColumns.size(); i++) {
            int fieldIndex = selectedIndexes[i];
            if (fieldIndex >= record.getArity()) {
                throw new IOException(
                        String.format(
                                "Row arity %d does not contain TVF column '%s' at index %d.",
                                record.getArity(), selectedColumns.get(i), fieldIndex));
            }
            Object field = rowConverter.convertExternal(record, fieldIndex);
            values.put(selectedColumns.get(i), field == null ? null : field.toString());
        }
        if (deletable) {
            values.put(TVF_DELETE_SIGN, parseDeleteSign(record.getRowKind()));
        }
        return objectMapper.writeValueAsString(values);
    }

    private static int[] resolveIndexes(String[] fieldNames, List<String> selectedColumns) {
        int[] indexes = new int[selectedColumns.size()];
        for (int i = 0; i < selectedColumns.size(); i++) {
            indexes[i] = findFieldIndex(fieldNames, selectedColumns.get(i));
        }
        return indexes;
    }

    private static int findFieldIndex(String[] fieldNames, String selectedColumn) {
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(selectedColumn)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown TVF column: " + selectedColumn);
    }
}
