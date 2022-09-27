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
package org.apache.doris.flink.catalog;

import org.apache.commons.compress.utils.Lists;
import org.apache.doris.flink.table.DorisDynamicTableFactory;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Predicate;

import static org.apache.doris.flink.catalog.DorisCatalogOptions.DEFAULT_DATABASE;
import static org.apache.doris.flink.catalog.DorisCatalogOptions.JDBCURL;
import static org.apache.doris.flink.table.DorisConfigOptions.FENODES;
import static org.apache.doris.flink.table.DorisConfigOptions.IDENTIFIER;
import static org.apache.doris.flink.table.DorisConfigOptions.PASSWORD;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_LABEL_PREFIX;
import static org.apache.doris.flink.table.DorisConfigOptions.TABLE_IDENTIFIER;
import static org.apache.doris.flink.table.DorisConfigOptions.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.util.Preconditions.checkArgument;

public class DorisCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(DorisCatalog.class);

    private static final Set<String> builtinDatabases =
            new HashSet<String>() {
                {
                    add("information_schema");
                }
            };

    private final String username;
    private final String password;
    private final String jdbcUrl;
    private final Map<String, String> properties;

    public DorisCatalog(
            String catalogName,
            String jdbcUrl,
            String defaultDatabase,
            String username,
            String password,
            Map<String, String> properties) {
        super(catalogName, defaultDatabase);

        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(jdbcUrl), "jdbc-url cannot be null or empty");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(username), "username cannot be null or empty");

        this.jdbcUrl = jdbcUrl.endsWith("/") ? jdbcUrl : jdbcUrl + "/";
        ;
        this.username = username;
        this.password = password;
        this.properties = Collections.unmodifiableMap(properties);
    }

    @Override
    public void open() throws CatalogException {
        // test connection, fail early if we cannot connect to database
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", jdbcUrl), e);
        }

        LOG.info("Catalog {} established connection to {}", getName(), jdbcUrl);
    }

    @Override
    public synchronized void close() throws CatalogException {
        try {
            LOG.info("Closed catalog {} ", getName());
        } catch (Exception e) {
            throw new CatalogException(String.format("Closing catalog %s failed.", getName()), e);
        }
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new DorisDynamicTableFactory());
    }

    // ------------- databases -------------

    @Override
    public List<String> listDatabases() throws CatalogException {
        return extractColumnValuesBySQL(
                jdbcUrl,
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));

        return listDatabases().contains(databaseName);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------------- tables -------------

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(
                org.apache.commons.lang3.StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return extractColumnValuesBySQL(
                jdbcUrl + databaseName,
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?",
                1,
                null,
                databaseName);
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        String databaseName = tablePath.getDatabaseName();
        String tableName = tablePath.getObjectName();
        Map<String, String> props = new HashMap<>(properties);
        props.put(CONNECTOR.key(), IDENTIFIER);
        if (!props.containsKey(FENODES.key())) {
            props.put(FENODES.key(), queryFenodes());
        }
        props.put(USERNAME.key(), username);
        props.put(PASSWORD.key(), password);
        props.put(TABLE_IDENTIFIER.key(), databaseName + "." + tableName);

        String labelPrefix = props.getOrDefault(SINK_LABEL_PREFIX.key(),"");
        props.put(SINK_LABEL_PREFIX.key(), String.join("_",labelPrefix,databaseName,tableName));
        //remove catalog option
        props.remove(JDBCURL.key());
        props.remove(DEFAULT_DATABASE.key());
        return CatalogTable.of(createTableSchema(databaseName, tableName), null, Lists.newArrayList(), props);

    }

    @VisibleForTesting
    protected String queryFenodes() {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            StringJoiner fenodes = new StringJoiner(",");
            PreparedStatement ps = conn.prepareStatement("SHOW FRONTENDS");
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String ip = resultSet.getString("IP");
                String port = resultSet.getString("HttpPort");
                fenodes.add(ip + ":" + port);
            }
            return fenodes.toString();
        } catch (Exception e) {
            throw new CatalogException("Failed getting fenodes", e);
        }
    }

    private Schema createTableSchema(String databaseName, String tableName) {
        String dbUrl = jdbcUrl + databaseName;
        try (Connection conn = DriverManager.getConnection(dbUrl, username, password)) {
            PreparedStatement ps =
                    conn.prepareStatement(
                            String.format("SELECT COLUMN_NAME,DATA_TYPE,COLUMN_SIZE,DECIMAL_DIGITS FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA`= '%s' AND `TABLE_NAME`= '%s'", databaseName, tableName));

            List<String> columnNames = new ArrayList<>();
            List<DataType> columnTypes = new ArrayList<>();
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                String columnType = resultSet.getString("DATA_TYPE");
                long columnSize = resultSet.getLong("COLUMN_SIZE");
                long columnDigit = resultSet.getLong("DECIMAL_DIGITS");
                DataType flinkType = DorisTypeMapper.toFlinkType(columnName, columnType, (int) columnSize, (int) columnDigit);
                columnNames.add(columnName);
                columnTypes.add(flinkType);
            }
            Schema.Builder schemaBuilder = Schema.newBuilder().fromFields(columnNames, columnTypes);
            Schema tableSchema = schemaBuilder.build();
            return tableSchema;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting catalog %s database %s table %s", getName(), databaseName, tableName), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName()).contains(tablePath.getObjectName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------------- partitions -------------

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
            PartitionSpecInvalidException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
            PartitionSpecInvalidException, PartitionAlreadyExistsException,
            CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------------- functions -------------

    @Override
    public List<String> listFunctions(String dbName)
            throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------------- statistics -------------

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }


    private List<String> extractColumnValuesBySQL(
            String connUrl,
            String sql,
            int columnIndex,
            Predicate<String> filterFunc,
            Object... params) {

        List<String> columnValues = Lists.newArrayList();

        try (Connection conn = DriverManager.getConnection(connUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
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
                            "The following SQL query could not be executed (%s): %s", connUrl, sql),
                    e);
        }
    }
}
