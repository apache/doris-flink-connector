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

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.commons.compress.utils.Lists;
import org.apache.doris.flink.cfg.DorisConnectionOptions;
import org.apache.doris.flink.connection.JdbcConnectionProvider;
import org.apache.doris.flink.connection.SimpleJdbcConnectionProvider;
import org.apache.doris.flink.exception.CreateTableException;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.DorisSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Doris System Operate. */
@Public
public class DorisSystem implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisSystem.class);
    private static final String TABLE_BUCKETS = "table-buckets";
    private final JdbcConnectionProvider jdbcConnectionProvider;
    private static final List<String> builtinDatabases =
            Collections.singletonList("information_schema");

    public DorisSystem(DorisConnectionOptions options) {
        this.jdbcConnectionProvider = new SimpleJdbcConnectionProvider(options);
    }

    public List<String> listDatabases() {
        return extractColumnValuesBySQL(
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    public boolean databaseExists(String database) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(database));
        return listDatabases().contains(database);
    }

    public boolean createDatabase(String database) {
        execute(String.format("CREATE DATABASE IF NOT EXISTS %s", database));
        return true;
    }

    public boolean dropDatabase(String database) {
        execute(String.format("DROP DATABASE IF EXISTS %s", database));
        return true;
    }

    public boolean tableExists(String database, String table) {
        return databaseExists(database) && listTables(database).contains(table);
    }

    public boolean columnExists(String database, String table, String columnName) {
        if (tableExists(database, table)) {
            List<String> columns =
                    extractColumnValuesBySQL(
                            "SELECT COLUMN_NAME FROM information_schema.`COLUMNS` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
                            1,
                            null,
                            database,
                            table,
                            columnName);
            if (columns != null && !columns.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public List<String> listTables(String databaseName) {
        if (!databaseExists(databaseName)) {
            throw new DorisRuntimeException("database" + databaseName + " is not exists");
        }
        return extractColumnValuesBySQL(
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?",
                1,
                null,
                databaseName);
    }

    public void dropTable(String tableName) {
        execute(String.format("DROP TABLE IF EXISTS %s", tableName));
    }

    public void createTable(TableSchema schema) {
        String ddl = generateDorisTableDdl(schema);
        LOG.info("Create table with ddl:{}", ddl);
        execute(ddl);
    }

    public void execute(String sql) {
        try (Statement statement =
                jdbcConnectionProvider.getOrEstablishConnection().createStatement()) {
            statement.execute(sql);
        } catch (Exception e) {
            throw new DorisSystemException(
                    String.format("SQL query could not be executed: %s", sql), e);
        }
    }

    public List<String> extractColumnValuesBySQL(
            String sql, int columnIndex, Predicate<String> filterFunc, Object... params) {

        List<String> columnValues = Lists.newArrayList();
        try (PreparedStatement ps =
                jdbcConnectionProvider.getOrEstablishConnection().prepareStatement(sql)) {
            if (Objects.nonNull(params) && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String columnValue = rs.getString(columnIndex);
                if (Objects.isNull(filterFunc) || filterFunc.test(columnValue)) {
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new DorisSystemException(
                    String.format("The following SQL query could not be executed: %s", sql), e);
        }
    }

    /**
     * Generate the DDL (Data Definition Language) for creating a Doris table based on the given
     * table schema.
     *
     * @param schema the table schema containing information about the table structure and
     *     properties
     * @return the DDL string for creating the Doris table
     */
    public static String generateDorisTableDdl(TableSchema schema) {
        StringBuilder sb = new StringBuilder();
        appendTableHeader(sb, schema);

        // append table columns.
        appendColumns(sb, schema);

        // append table comment
        appendTableComment(sb, schema.getTableComment());

        // append table model
        appendTableModel(sb, schema.getModel(), schema.getKeys());

        // append distribute key
        appendDistributeKey(sb, schema.getDistributeKeys());

        // append buckets
        appendBuckets(sb, schema.getTableBuckets());

        // append properties
        appendProperties(sb, schema.getProperties());
        return sb.toString();
    }

    private static void appendTableHeader(StringBuilder sb, TableSchema schema) {
        sb.append("CREATE TABLE IF NOT EXISTS ")
                .append(identifier(schema.getDatabase()))
                .append(".")
                .append(identifier(schema.getTable()))
                .append("(");
    }

    /**
     * Append column definitions to the SQL string based on the given table schema.
     *
     * @param sql the StringBuilder to append the SQL string
     * @param schema the table schema containing the fields and keys
     */
    private static void appendColumns(StringBuilder sql, TableSchema schema) {
        Map<String, FieldSchema> fields = schema.getFields();
        List<String> keys = schema.getKeys();

        for (String key : keys) {
            if (!fields.containsKey(key)) {
                throw new CreateTableException("Key " + key + " not found in column list");
            }
            FieldSchema field = fields.get(key);
            appendColumn(sql, field, true);
        }

        for (Map.Entry<String, FieldSchema> entry : fields.entrySet()) {
            if (keys.contains(entry.getKey())) {
                continue;
            }
            FieldSchema field = entry.getValue();
            appendColumn(sql, field, false);
        }
        if (sql.charAt(sql.length() - 1) == ',') {
            sql.deleteCharAt(sql.length() - 1);
        }
        sql.append(" ) ");
    }

    /**
     * Append a column definition to the SQL string.
     *
     * @param sql the StringBuilder to append the SQL string
     * @param field the field schema representing the column
     * @param isKey flag indicating if the column is part of the primary key
     */
    private static void appendColumn(StringBuilder sql, FieldSchema field, boolean isKey) {
        String fieldType = field.getTypeString();
        if (isKey && DorisType.STRING.equals(fieldType)) {
            fieldType = String.format("%s(%s)", DorisType.VARCHAR, 65533);
        }
        sql.append(identifier(field.getName())).append(" ").append(fieldType);

        if (field.getDefaultValue() != null) {
            sql.append(" DEFAULT ").append(quoteDefaultValue(field.getDefaultValue()));
        }
        sql.append(" COMMENT '").append(quoteComment(field.getComment())).append("',");
    }

    private static void appendTableModel(StringBuilder sb, DataModel dataModel, List<String> keys) {
        if (DataModel.UNIQUE.equals(dataModel)) {
            sb.append(dataModel.name())
                    .append(" KEY(")
                    .append(String.join(",", identifier(keys)))
                    .append(")");
        }
    }

    private static void appendTableComment(StringBuilder sb, String tableComment) {
        if (!StringUtils.isNullOrWhitespaceOnly(tableComment)) {
            sb.append(" COMMENT '").append(quoteComment(tableComment)).append("' ");
        }
    }

    private static void appendDistributeKey(StringBuilder sb, List<String> distributeKeys) {
        sb.append(" DISTRIBUTED BY HASH(")
                .append(String.join(",", identifier(distributeKeys)))
                .append(")");
    }

    /**
     * Append the BUCKETS clause to the DDL string. If the number of buckets is provided, append the
     * specified number of buckets, otherwise append 'BUCKETS AUTO' to let Doris determine the
     * number of buckets automatically.
     *
     * @param sb the StringBuilder to append the DDL string
     * @param buckets the number of buckets, or null if 'BUCKETS AUTO' should be used
     * @throws CreateTableException if the number of buckets is not positive
     */
    private static void appendBuckets(StringBuilder sb, Integer buckets) {
        if (buckets != null) {
            if (buckets <= 0) {
                throw new CreateTableException("The number of buckets must be positive.");
            }
            sb.append(" BUCKETS ").append(buckets);
        } else {
            sb.append(" BUCKETS AUTO ");
        }
    }

    /**
     * Append the table properties of Doris.
     *
     * @param sb the StringBuilder to append the DDL string
     * @param properties the properties of the Doris table
     */
    private static void appendProperties(StringBuilder sb, Map<String, String> properties) {
        if (!properties.isEmpty()) {
            sb.append(" PROPERTIES (");
            sb.append(
                    properties.entrySet().stream()
                            .filter(key -> !key.getKey().equals(TABLE_BUCKETS))
                            .map(
                                    entry ->
                                            quoteProperties(entry.getKey())
                                                    + "="
                                                    + quoteProperties(entry.getValue()))
                            .collect(Collectors.joining(",")));
            sb.append(")");
        }
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

    private static List<String> identifier(List<String> name) {
        return name.stream().map(DorisSystem::identifier).collect(Collectors.toList());
    }

    public static String identifier(String name) {
        return "`" + name + "`";
    }

    public static String quoteTableIdentifier(String tableIdentifier) {
        String[] dbTable = tableIdentifier.split("\\.");
        Preconditions.checkArgument(dbTable.length == 2);
        return identifier(dbTable[0]) + "." + identifier(dbTable[1]);
    }

    private static String quoteProperties(String name) {
        return "'" + name + "'";
    }
}
