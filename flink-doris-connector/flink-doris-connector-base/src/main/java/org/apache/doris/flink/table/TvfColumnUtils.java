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

package org.apache.doris.flink.table;

import org.apache.flink.table.api.ValidationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.doris.flink.sink.writer.LoadConstants.COLUMNS_KEY;

/** Resolves the fixed column list used by the TVF write path. */
final class TvfColumnUtils {

    private TvfColumnUtils() {}

    static List<String> resolveColumns(Properties loadProperties, String[] schemaFieldNames) {
        String configuredColumns = loadProperties.getProperty(COLUMNS_KEY);
        if (configuredColumns == null || configuredColumns.trim().isEmpty()) {
            return Arrays.asList(schemaFieldNames);
        }

        Set<String> schemaFields = new HashSet<>(Arrays.asList(schemaFieldNames));
        Set<String> seen = new HashSet<>();
        List<String> columns = new ArrayList<>();
        for (String rawColumn : configuredColumns.split(",", -1)) {
            String column = unquoteIdentifier(rawColumn.trim());
            if (column.isEmpty()) {
                throw new ValidationException(
                        "Option 'sink.properties.columns' contains an empty column.");
            }
            if (!schemaFields.contains(column)) {
                throw new ValidationException(
                        String.format(
                                "Column '%s' from 'sink.properties.columns' is not a physical column. "
                                        + "TVF write mode only supports fixed physical columns.",
                                column));
            }
            if (!seen.add(column)) {
                throw new ValidationException(
                        String.format(
                                "Column '%s' is duplicated in 'sink.properties.columns'.", column));
            }
            columns.add(column);
        }
        return columns;
    }

    private static String unquoteIdentifier(String value) {
        if (value.length() >= 2 && value.startsWith("`") && value.endsWith("`")) {
            return value.substring(1, value.length() - 1).replace("``", "`");
        }
        return value;
    }
}
