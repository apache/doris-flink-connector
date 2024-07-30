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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.doris.flink.exception.CreateTableException;
import org.apache.doris.flink.tools.cdc.DorisTableConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
            DorisTableConfig dorisTableConfig,
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
        if (Objects.nonNull(dorisTableConfig)) {
            tableSchema.setProperties(dorisTableConfig.getTableProperties());
            tableSchema.setTableBuckets(
                    parseTableSchemaBuckets(dorisTableConfig.getTableBuckets(), table));
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
        if (MapUtils.isNotEmpty(tableBucketsMap)) {
            // Firstly, if the table name is in the table-buckets map, set the buckets of the table.
            if (tableBucketsMap.containsKey(tableName)) {
                return tableBucketsMap.get(tableName);
            }
            // Secondly, iterate over the map to find a corresponding regular expression match.
            for (Entry<String, Integer> entry : tableBucketsMap.entrySet()) {
                Pattern pattern = Pattern.compile(entry.getKey());
                if (pattern.matcher(tableName).matches()) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    public static String generateCreateTableDDL(TableSchema schema) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sb.append(identifier(schema.getDatabase()))
                .append(".")
                .append(identifier(schema.getTable()))
                .append("(");

        Map<String, FieldSchema> fields = schema.getFields();
        List<String> keys = schema.getKeys();
        // append keys
        for (String key : keys) {
            if (!fields.containsKey(key)) {
                throw new CreateTableException("key " + key + " not found in column list");
            }
            FieldSchema field = fields.get(key);
            buildColumn(sb, field, true);
        }

        // append values
        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            if (keys.contains(entry.getKey())) {
                continue;
            }
            FieldSchema field = entry.getValue();
            buildColumn(sb, field, false);
        }
        sb = sb.deleteCharAt(sb.length() - 1);
        sb.append(" ) ");
        // append uniq model
        if (DataModel.UNIQUE.equals(schema.getModel())) {
            sb.append(schema.getModel().name())
                    .append(" KEY(")
                    .append(String.join(",", identifier(schema.getKeys())))
                    .append(")");
        }

        // append table comment
        if (!StringUtils.isNullOrWhitespaceOnly(schema.getTableComment())) {
            sb.append(" COMMENT '").append(quoteComment(schema.getTableComment())).append("' ");
        }

        // append distribute key
        sb.append(" DISTRIBUTED BY HASH(")
                .append(String.join(",", identifier(schema.getDistributeKeys())))
                .append(")");

        Map<String, String> properties = schema.getProperties();
        if (schema.getTableBuckets() != null) {

            int bucketsNum = schema.getTableBuckets();
            if (bucketsNum <= 0) {
                throw new CreateTableException("The number of buckets must be positive.");
            }
            sb.append(" BUCKETS ").append(bucketsNum);
        } else {
            sb.append(" BUCKETS AUTO ");
        }
        // append properties
        int index = 0;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (index == 0) {
                sb.append(" PROPERTIES (");
            }
            if (index > 0) {
                sb.append(",");
            }
            sb.append(quoteProperties(entry.getKey()))
                    .append("=")
                    .append(quoteProperties(entry.getValue()));
            index++;

            if (index == (schema.getProperties().size())) {
                sb.append(")");
            }
        }
        return sb.toString();
    }

    private static void buildColumn(StringBuilder sql, FieldSchema field, boolean isKey) {
        String fieldType = field.getTypeString();
        if (isKey && DorisType.STRING.equals(fieldType)) {
            fieldType = String.format("%s(%s)", DorisType.VARCHAR, 65533);
        }
        sql.append(identifier(field.getName())).append(" ").append(fieldType);

        if (field.getDefaultValue() != null) {
            sql.append(" DEFAULT " + quoteDefaultValue(field.getDefaultValue()));
        }
        sql.append(" COMMENT '").append(quoteComment(field.getComment())).append("',");
    }

    private static String quoteProperties(String name) {
        return "'" + name + "'";
    }

    private static List<String> identifier(List<String> names) {
        return names.stream().map(DorisSchemaFactory::identifier).collect(Collectors.toList());
    }

    public static String identifier(String name) {
        if (name.startsWith("`") && name.endsWith("`")) {
            return name;
        }
        return "`" + name + "`";
    }

    public static String quoteDefaultValue(String defaultValue) {
        // DEFAULT current_timestamp not need quote
        if (defaultValue.equalsIgnoreCase("current_timestamp")) {
            return defaultValue;
        }
        return "'" + defaultValue + "'";
    }

    public static String quoteComment(String comment) {
        if (comment == null) {
            return "";
        } else {
            return comment.replaceAll("'", "\\\\'");
        }
    }

    public static String quoteTableIdentifier(String tableIdentifier) {
        String[] dbTable = tableIdentifier.split("\\.");
        Preconditions.checkArgument(dbTable.length == 2);
        return identifier(dbTable[0]) + "." + identifier(dbTable[1]);
    }
}
