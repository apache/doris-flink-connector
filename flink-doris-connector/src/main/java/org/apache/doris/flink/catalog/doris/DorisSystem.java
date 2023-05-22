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


import org.apache.commons.compress.utils.Lists;
import org.apache.doris.flink.cfg.DorisConnectionOptions;
import org.apache.doris.flink.connection.JdbcConnectionProvider;
import org.apache.doris.flink.connection.SimpleJdbcConnectionProvider;
import org.apache.doris.flink.exception.CreateTableException;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.tools.cdc.DatabaseSync;
import org.apache.flink.annotation.Public;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Doris System Operate
 */
@Public
public class DorisSystem {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSync.class);
    private JdbcConnectionProvider jdbcConnectionProvider;
    private static final List<String> builtinDatabases = Arrays.asList("information_schema");

    public DorisSystem(DorisConnectionOptions options) {
        this.jdbcConnectionProvider = new SimpleJdbcConnectionProvider(options);
    }

    public List<String> listDatabases() throws Exception {
        return extractColumnValuesBySQL(
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    public boolean databaseExists(String database) throws Exception {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(database));
        return listDatabases().contains(database);
    }

    public boolean createDatabase(String database) throws Exception {
        execute(String.format("CREATE DATABASE %s", database));
        return true;
    }

    public boolean tableExists(String database, String table){
        try {
            return databaseExists(database)
                    && listTables(database).contains(table);
        } catch (Exception e) {
            return false;
        }
    }

    public List<String> listTables(String databaseName) throws Exception {
        if (!databaseExists(databaseName)) {
            throw new DorisRuntimeException("database" + databaseName + " is not exists");
        }
        return extractColumnValuesBySQL(
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?",
                1,
                null,
                databaseName);
    }

    public void createTable(TableSchema schema) throws Exception {
        String ddl = buildCreateTableDDL(schema);
        LOG.info("Create table with ddl:{}", ddl);
        execute(ddl);
    }

    public void execute(String sql) throws Exception {
        Connection conn = jdbcConnectionProvider.getOrEstablishConnection();
        try (Statement statement = conn.createStatement()) {
            statement.execute(sql);
        }
    }

    private List<String> extractColumnValuesBySQL(
            String sql,
            int columnIndex,
            Predicate<String> filterFunc,
            Object... params) throws Exception {

        Connection conn = jdbcConnectionProvider.getOrEstablishConnection();
        List<String> columnValues = Lists.newArrayList();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
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
            throw new CatalogException(
                    String.format(
                            "The following SQL query could not be executed: %s", sql),
                    e);
        }
    }

    public String buildCreateTableDDL(TableSchema schema) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sb.append(identifier(schema.getDatabase()))
                .append(".")
                .append(identifier(schema.getTable()))
                .append("(");

        Map<String, String> fields = schema.getFields();
        List<String> keys = schema.getKeys();
        //append keys
        for(String key : keys){
            if(!fields.containsKey(key)){
                throw new CreateTableException("key " + key + " not found in column list");
            }
            sb.append(identifier(key))
                    .append(" ")
                    .append(fields.get(key))
                    .append(",");
        }

        //append values
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            if(keys.contains(entry.getKey())){
                continue;
            }
            sb.append(identifier(entry.getKey()))
                    .append(" ")
                    .append(entry.getValue())
                    .append(",");
        }
        sb = sb.deleteCharAt(sb.length() -1);
        sb.append(" ) ");
        //append model
        sb.append(schema.getModel().name())
                .append(" KEY(")
                .append(String.join(",", schema.getKeys()))
                .append(")");
        //append distribute key
        sb.append(" DISTRIBUTED BY HASH(")
                .append(String.join(",", schema.getDistributeKeys()))
                .append(") BUCKETS AUTO ");

        //append properties
        int index = 0;
        for (Map.Entry<String, String> entry : schema.getProperties().entrySet()) {
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

            if (index == schema.getProperties().size()) {
                sb.append(")");
            }
        }
        return sb.toString();
    }

    private String identifier(String name) {
        return "`" + name + "`";
    }

    private String quoteProperties(String name) {
        return "'" + name + "'";
    }

}
