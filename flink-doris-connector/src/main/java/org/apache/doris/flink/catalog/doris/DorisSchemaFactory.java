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

package org.apache.doris.flink.catalog.doris;

import org.apache.flink.annotation.VisibleForTesting;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

/**
 * Factory that creates doris schema.
 *
 * <p>In the case where doris schema needs to be created, it is best to create it through this
 * factory
 */
public class DorisSchemaFactory {

    public static TableSchema createTableSchema(
            String database,
            String table,
            Map<String, FieldSchema> columnFields,
            List<String> pkKeys,
            Map<String, String> tableProperties,
            String tableComment) {
        TableSchema tableSchema = new TableSchema();
        tableSchema.setDatabase(database);
        tableSchema.setTable(table);
        tableSchema.setModel(
                CollectionUtils.isEmpty(pkKeys) ? DataModel.DUPLICATE : DataModel.UNIQUE);
        tableSchema.setFields(columnFields);
        tableSchema.setKeys(buildKeys(pkKeys, columnFields));
        tableSchema.setTableComment(tableComment);
        tableSchema.setDistributeKeys(buildDistributeKeys(pkKeys, columnFields));
        tableSchema.setProperties(tableProperties);
        if (tableProperties.containsKey("table-buckets")) {
            String tableBucketsConfig = tableProperties.get("table-buckets");
            Map<String, Integer> tableBuckets = buildTableBucketMap(tableBucketsConfig);
            Integer buckets = parseTableSchemaBuckets(tableBuckets, tableSchema.getTable());
            tableSchema.setTableBuckets(buckets);
        }
        return tableSchema;
    }

    private static List<String> buildDistributeKeys(
            List<String> primaryKeys, Map<String, FieldSchema> fields) {
        return buildKeys(primaryKeys, fields);
    }

    /**
     * Theoretically, the duplicate table of doris does not need to distinguish the key column, but
     * in the actual table creation statement, the key column will be automatically added. So if it
     * is a duplicate table, primaryKeys is empty, and we uniformly take the first field as the key.
     */
    private static List<String> buildKeys(
            List<String> primaryKeys, Map<String, FieldSchema> fields) {
        if (CollectionUtils.isNotEmpty(primaryKeys)) {
            return primaryKeys;
        }
        if (!fields.isEmpty()) {
            Entry<String, FieldSchema> firstField = fields.entrySet().iterator().next();
            return Collections.singletonList(firstField.getKey());
        }
        return new ArrayList<>();
    }

    @VisibleForTesting
    public static Integer parseTableSchemaBuckets(
            Map<String, Integer> tableBucketsMap, String tableName) {
        if (tableBucketsMap != null) {
            // Firstly, if the table name is in the table-buckets map, set the buckets of the table.
            if (tableBucketsMap.containsKey(tableName)) {
                return tableBucketsMap.get(tableName);
            }
            // Secondly, iterate over the map to find a corresponding regular expression match,
            for (Entry<String, Integer> entry : tableBucketsMap.entrySet()) {

                Pattern pattern = Pattern.compile(entry.getKey());
                if (pattern.matcher(tableName).matches()) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    /**
     * Build table bucket Map.
     *
     * @param tableBuckets the string of tableBuckets, eg:student:10,student_info:20,student.*:30
     * @return The table name and buckets map. The key is table name, the value is buckets.
     */
    @VisibleForTesting
    public static Map<String, Integer> buildTableBucketMap(String tableBuckets) {
        Map<String, Integer> tableBucketsMap = new LinkedHashMap<>();
        String[] tableBucketsArray = tableBuckets.split(",");
        for (String tableBucket : tableBucketsArray) {
            String[] tableBucketArray = tableBucket.split(":");
            tableBucketsMap.put(
                    tableBucketArray[0].trim(), Integer.parseInt(tableBucketArray[1].trim()));
        }
        return tableBucketsMap;
    }
}
