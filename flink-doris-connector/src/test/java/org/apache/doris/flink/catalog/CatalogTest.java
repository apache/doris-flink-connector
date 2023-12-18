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
@Ignore
public class CatalogTest {
    private static final String TEST_CATALOG_NAME = "doris_catalog";
    private static final String TEST_FENODES = "127.0.0.1:8030";
    private static final String TEST_JDBCURL = "jdbc:mysql://127.0.0.1:9030";
    private static final String TEST_USERNAME = "root";
    private static final String TEST_PWD = "";
    private static final String TEST_DB = "db1";
    private static final String TEST_TABLE = "t_all_types";
    private static final String TEST_TABLE_SINK = "t_all_types_sink";
    private static final String TEST_TABLE_SINK_GROUPBY = "t_all_types_sink_groupby";

    protected static final Schema TABLE_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.STRING())
                    .column("c_boolean", DataTypes.BOOLEAN())
                    .column("c_char", DataTypes.CHAR(1))
                    .column("c_date", DataTypes.DATE())
                    .column("c_datetime", DataTypes.TIMESTAMP(0))
                    .column("c_decimal", DataTypes.DECIMAL(10, 2))
                    .column("c_double", DataTypes.DOUBLE())
                    .column("c_float", DataTypes.FLOAT())
                    .column("c_int", DataTypes.INT())
                    .column("c_bigint", DataTypes.BIGINT())
                    .column("c_largeint", DataTypes.STRING())
                    .column("c_smallint", DataTypes.SMALLINT())
                    .column("c_string", DataTypes.STRING())
                    .column("c_tinyint", DataTypes.TINYINT())
                    .build();

    protected static final TableSchema TABLE_SCHEMA_1 =
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
    public void setup() {
        DorisConnectionOptions connectionOptions =
                new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                        .withFenodes(TEST_FENODES)
                        .withJdbcUrl(TEST_JDBCURL)
                        .withUsername(TEST_USERNAME)
                        .withPassword(TEST_PWD)
                        .build();

        Map<String, String> props = new HashMap<>();
        props.put("sink.enable-2pc", "false");
        catalog = new DorisCatalog(TEST_CATALOG_NAME, connectionOptions, TEST_DB, props);
        this.tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        // Use doris catalog.
        tEnv.registerCatalog(TEST_CATALOG_NAME, catalog);
        tEnv.useCatalog(TEST_CATALOG_NAME);
    }

    @Test
    public void testQueryFenodes() {
        String actual = catalog.queryFenodes();
        assertEquals("127.0.0.1:8030", actual);
    }

    @Test
    public void testListDatabases() {
        List<String> actual = catalog.listDatabases();
        assertEquals(Collections.singletonList(TEST_DB), actual);
    }

    @Test
    public void testDbExists() throws Exception {
        String databaseNotExist = "nonexistent";
        assertFalse(catalog.databaseExists(databaseNotExist));
        assertTrue(catalog.databaseExists(TEST_DB));
    }

    @Test
    public void testCreateDb() throws Exception {
        catalog.createDatabase("db1", createDb(), true);
        assertTrue(catalog.databaseExists("db1"));
    }

    @Test
    public void testDropDb() throws Exception {
        catalog.dropDatabase("db1", false);
        assertFalse(catalog.databaseExists("db1"));
    }

    @Test
    public void testListTables() throws DatabaseNotExistException {
        List<String> actual = catalog.listTables(TEST_DB);
        assertEquals(Arrays.asList(TEST_TABLE, TEST_TABLE_SINK, TEST_TABLE_SINK_GROUPBY), actual);
    }

    @Test
    public void testTableExists() {
        String tableNotExist = "nonexist";
        assertFalse(catalog.tableExists(new ObjectPath(TEST_DB, tableNotExist)));
    }

    @Test
    @Ignore
    public void testGetTable() throws TableNotExistException {
        // todo: string varchar mapping
        CatalogBaseTable table = catalog.getTable(new ObjectPath(TEST_DB, TEST_TABLE));
        System.out.println(table);
        assertEquals(TABLE_SCHEMA, table.getUnresolvedSchema());
    }

    @Test
    @Ignore
    public void testCreateTable()
            throws TableNotExistException, TableAlreadyExistException, DatabaseNotExistException {
        // todo: Record primary key not null information
        ObjectPath tablePath = new ObjectPath(TEST_DB, TEST_TABLE);
        catalog.dropTable(tablePath, true);
        catalog.createTable(tablePath, createTable(), true);
        CatalogBaseTable tableGet = catalog.getTable(tablePath);
        System.out.println(tableGet.getUnresolvedSchema());
        System.out.println(TABLE_SCHEMA_1);
        assertEquals(TABLE_SCHEMA_1, tableGet.getUnresolvedSchema());
    }

    @Test
    public void testDropTable() throws TableNotExistException {
        catalog.dropTable(new ObjectPath("db1", "tbl1"), true);
        assertFalse(catalog.tableExists(new ObjectPath("db1", "tbl1")));
    }

    // ------ test select query. ------

    @Test
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
    public void testWithoutCatalogDB() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TEST_TABLE))
                                .execute()
                                .collect());
        assertEquals(ALL_TYPES_ROWS, results);
    }

    @Test
    public void testWithoutCatalog() {
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from `%s`.`%s`", TEST_DB, TEST_TABLE))
                                .execute()
                                .collect());
        assertEquals(ALL_TYPES_ROWS, results);
    }

    @Test
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
                TABLE_SCHEMA_1,
                new HashMap<String, String>() {
                    {
                        put("connector", "doris");
                        put("table.properties.replication_num", "1");
                    }
                },
                "FlinkTable");
    }
}
