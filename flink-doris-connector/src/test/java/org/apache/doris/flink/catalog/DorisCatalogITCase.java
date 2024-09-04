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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import com.google.common.collect.Lists;
import org.apache.doris.flink.cfg.DorisConnectionOptions;
import org.apache.doris.flink.container.AbstractITCaseService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Class for unit tests to run on catalogs. */
public class DorisCatalogITCase extends AbstractITCaseService {
    private static final String TEST_CATALOG_NAME = "doris_catalog";
    private static final String TEST_DB = "catalog_db";
    private static final String TEST_TABLE = "t_all_types";
    private static final String TEST_TABLE_SINK = "t_all_types_sink";
    private static final String TEST_TABLE_SINK_GROUPBY = "t_all_types_sink_groupby";

    private static final TableSchema TABLE_SCHEMA =
            TableSchema.builder()
                    .field("id", new AtomicDataType(new VarCharType(false, 128)))
                    .field("c_boolean", DataTypes.BOOLEAN())
                    .field("c_char", DataTypes.CHAR(1))
                    .field("c_date", DataTypes.DATE())
                    .field("c_datetime", DataTypes.TIMESTAMP(0))
                    .field("c_decimal", DataTypes.DECIMAL(10, 2))
                    .field("c_double", DataTypes.DOUBLE())
                    .field("c_float", DataTypes.FLOAT())
                    .field("c_int", DataTypes.INT())
                    .field("c_bigint", DataTypes.BIGINT())
                    .field("c_largeint", DataTypes.STRING())
                    .field("c_smallint", DataTypes.SMALLINT())
                    .field("c_string", DataTypes.STRING())
                    .field("c_tinyint", DataTypes.TINYINT())
                    .field("c_array", DataTypes.ARRAY(DataTypes.INT()))
                    .field("c_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                    .field("c_row", DataTypes.ROW())
                    .field("c_varbinary", DataTypes.VARBINARY(16))
                    .primaryKey("id")
                    .build();

    private static final List<Row> ALL_TYPES_ROWS =
            Lists.newArrayList(
                    Row.ofKind(
                            RowKind.INSERT,
                            "100001",
                            true,
                            "a",
                            Date.valueOf("2022-08-31").toLocalDate(),
                            Timestamp.valueOf("2022-08-31 11:12:13").toLocalDateTime(),
                            BigDecimal.valueOf(1.12).setScale(2),
                            1.1234d,
                            1.1f,
                            1234567,
                            1234567890L,
                            "123456790123456790",
                            Short.parseShort("10"),
                            "catalog",
                            Byte.parseByte("1")),
                    Row.ofKind(
                            RowKind.INSERT,
                            "100002",
                            true,
                            "a",
                            Date.valueOf("2022-08-31").toLocalDate(),
                            Timestamp.valueOf("2022-08-31 11:12:13").toLocalDateTime(),
                            BigDecimal.valueOf(1.12).setScale(2),
                            1.1234d,
                            1.1f,
                            1234567,
                            1234567890L,
                            "123456790123456790",
                            Short.parseShort("10"),
                            "catalog",
                            Byte.parseByte("1")));

    private DorisCatalog catalog;
    private TableEnvironment tEnv;

    @Before
    public void setup()
            throws DatabaseAlreadyExistException, TableAlreadyExistException,
                    TableNotExistException, DatabaseNotExistException {
        DorisConnectionOptions connectionOptions =
                new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                        .withFenodes(getFenodes())
                        .withJdbcUrl(getDorisQueryUrl())
                        .withUsername(getDorisUsername())
                        .withPassword(getDorisPassword())
                        .build();

        Map<String, String> props = new HashMap<>();
        props.put("sink.enable-2pc", "false");
        catalog = new DorisCatalog(TEST_CATALOG_NAME, connectionOptions, TEST_DB, props);
        this.tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        // Use doris catalog.
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);

        catalog.createDatabase(TEST_DB, createDb(), true);
        catalog.createTable(new ObjectPath(TEST_DB, TEST_TABLE), createTable(), true);
        catalog.createTable(new ObjectPath(TEST_DB, TEST_TABLE_SINK), createTable(), true);
        catalog.createTable(new ObjectPath(TEST_DB, TEST_TABLE_SINK_GROUPBY), createTable(), true);
    }

    @Test
    @Ignore
    public void testQueryFenodes() {
        String actual = catalog.queryFenodes();
        assertEquals(getFenodes(), actual);
    }

    @Test
    public void testListDatabases() {
        List<String> actual = catalog.listDatabases();
        assertTrue(actual.contains(TEST_DB));
    }

    @Test
    public void testCreateDb() throws Exception {
        catalog.createDatabase("test_create", createDb(), true);
        assertTrue(catalog.databaseExists("test_create"));

        String databaseNotExist = "nonexistent";
        assertFalse(catalog.databaseExists(databaseNotExist));

        catalog.dropDatabase("test_create", false);
        assertFalse(catalog.databaseExists("test_create"));
    }

    @Test(expected = DatabaseAlreadyExistException.class)
    public void testCreateDbExists() throws Exception {
        catalog.createDatabase("test_create_exist", createDb(), true);
        assertTrue(catalog.databaseExists("test_create_exist"));

        catalog.createDatabase("test_create_exist", createDb(), false);
    }

    @Test(expected = DatabaseNotExistException.class)
    public void testDropDb() throws Exception {
        catalog.createDatabase("test_drop", createDb(), true);
        assertTrue(catalog.databaseExists("test_drop"));

        catalog.dropDatabase("test_drop", true);
        catalog.dropDatabase("test_drop", true);
        catalog.dropDatabase("test_drop", false);
    }

    @Test(expected = DatabaseNotExistException.class)
    public void testGetDatabase() throws Exception {
        catalog.createDatabase("test_get", createDb(), true);
        CatalogDatabase testGet = catalog.getDatabase("test_get");
        Assert.assertNotNull(testGet);
        catalog.dropDatabase("test_get", false);
        catalog.getDatabase("test_get");
    }

    @Test
    public void testListTables() throws DatabaseNotExistException {
        List<String> actual = catalog.listTables(TEST_DB);
        Collections.sort(actual);
        List<String> excepted = Arrays.asList(TEST_TABLE, TEST_TABLE_SINK, TEST_TABLE_SINK_GROUPBY);
        Collections.sort(excepted);
        assertEquals(excepted, actual);
    }

    @Test(expected = DatabaseNotEmptyException.class)
    public void testDropTableNotEmpty()
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        catalog.dropDatabase(TEST_DB, true, false);
    }

    @Test(expected = DatabaseNotExistException.class)
    public void testListTablesFromNoExistsDB() throws DatabaseNotExistException {
        catalog.listTables("db_no_exists");
    }

    @Test
    public void testTableExists() {
        String tableNotExist = "nonexist";
        assertFalse(catalog.tableExists(new ObjectPath(TEST_DB, tableNotExist)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDatabaseExists() {
        assertFalse(catalog.databaseExists(""));
    }

    @Test
    public void testGetTable() throws TableNotExistException {
        // todo: string varchar mapping
        CatalogBaseTable table = catalog.getTable(new ObjectPath(TEST_DB, TEST_TABLE));
        Schema actual = table.getUnresolvedSchema();
        assertEquals(
                TABLE_SCHEMA.getFieldNames(),
                actual.getColumns().stream().map(Schema.UnresolvedColumn::getName).toArray());
    }

    @Test(expected = TableNotExistException.class)
    public void testGetTableNoExists() throws TableNotExistException {
        catalog.getTable(new ObjectPath("no_exist_db", "no_exist_tbl"));
    }

    @Test
    public void testDropTable()
            throws TableNotExistException, TableAlreadyExistException, DatabaseNotExistException {
        catalog.createTable(new ObjectPath(TEST_DB, "drop_table"), createTable(), true);
        Assert.assertTrue(catalog.tableExists(new ObjectPath(TEST_DB, "drop_table")));
        catalog.dropTable(new ObjectPath(TEST_DB, "drop_table"), true);

        catalog.dropTable(new ObjectPath("db1", "tbl1"), true);
        assertFalse(catalog.tableExists(new ObjectPath("db1", "tbl1")));
    }

    @Test(expected = TableNotExistException.class)
    public void testDropTableNoExist() throws TableNotExistException {
        catalog.dropTable(new ObjectPath("no_exists_db", "no_exist_tbl"), true);
        catalog.dropTable(new ObjectPath("no_exists_db", "no_exist_tbl"), false);
    }

    @Test(expected = DatabaseNotExistException.class)
    public void testCreateTableDbNoExists()
            throws TableAlreadyExistException, DatabaseNotExistException {
        catalog.createTable(new ObjectPath("no_exists_db", "create_table"), createTable(), true);
    }

    @Test
    public void testCreateTable() throws TableAlreadyExistException, DatabaseNotExistException {
        CatalogTableImpl catalogTable =
                new CatalogTableImpl(
                        TABLE_SCHEMA,
                        new HashMap<String, String>() {
                            {
                                put("connector", "doris-1");
                                put("table.properties.replication_num", "1");
                            }
                        },
                        "FlinkTable");
        catalog.createTable(
                new ObjectPath(TEST_DB, "create_table_wrong_connector"), catalogTable, true);
        boolean exists =
                catalog.tableExists(new ObjectPath(TEST_DB, "create_table_wrong_connector"));
        assertFalse(exists);
        catalog.createTable(new ObjectPath(TEST_DB, TEST_TABLE), createTable(), true);
    }

    // ------ test select query. ------

    @Test
    @Ignore
    public void testSelectField() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select id from %s", TEST_TABLE))
                                .execute()
                                .collect());
        assertEquals(
                Lists.newArrayList(
                        Row.ofKind(RowKind.INSERT, "100001"), Row.ofKind(RowKind.INSERT, "100002")),
                results);
    }

    @Test
    @Ignore
    public void testWithoutCatalogDB() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TEST_TABLE))
                                .execute()
                                .collect());
        assertEquals(ALL_TYPES_ROWS, results);
    }

    @Test
    @Ignore
    public void testWithoutCatalog() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from `%s`.`%s`", TEST_DB, TEST_TABLE))
                                .execute()
                                .collect());
        assertEquals(ALL_TYPES_ROWS, results);
    }

    @Test
    @Ignore
    public void testFullPath() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(
                                        String.format(
                                                "select * from %s.%s.`%s`",
                                                TEST_CATALOG_NAME,
                                                catalog.getDefaultDatabase(),
                                                TEST_TABLE))
                                .execute()
                                .collect());
        assertEquals(ALL_TYPES_ROWS, results);
    }

    @Test
    @Ignore
    public void testSelectToInsert() throws Exception {

        String sql =
                String.format("insert into `%s` select * from `%s`", TEST_TABLE_SINK, TEST_TABLE);
        tEnv.executeSql(sql).await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TEST_TABLE_SINK))
                                .execute()
                                .collect());
        assertEquals(ALL_TYPES_ROWS, results);
    }

    @Test
    @Ignore
    public void testGroupByInsert() throws Exception {
        // Changes primary key for the next record.
        tEnv.executeSql(
                        String.format(
                                "insert into `%s` select  `c_string`, max(`id`) `id` from `%s` "
                                        + "group by `c_string` ",
                                TEST_TABLE_SINK_GROUPBY, TEST_TABLE))
                .await();

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from `%s`", TEST_TABLE_SINK_GROUPBY))
                                .execute()
                                .collect());
        assertEquals(Lists.newArrayList(Row.ofKind(RowKind.INSERT, "catalog", "100002")), results);
    }

    private static CatalogDatabase createDb() {
        return new CatalogDatabaseImpl(
                new HashMap<String, String>() {
                    {
                        put("k1", "v1");
                    }
                },
                "");
    }

    private static CatalogTable createTable() {
        return new CatalogTableImpl(
                TABLE_SCHEMA,
                new HashMap<String, String>() {
                    {
                        put("connector", "doris");
                        put("table.properties.replication_num", "1");
                    }
                },
                "FlinkTable");
    }
}
