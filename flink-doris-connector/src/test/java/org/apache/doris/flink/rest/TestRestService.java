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

package org.apache.doris.flink.rest;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.exception.ShouldNeverHappenException;
import org.apache.doris.flink.rest.models.BackendV2;
import org.apache.doris.flink.rest.models.Field;
import org.apache.doris.flink.rest.models.QueryPlan;
import org.apache.doris.flink.rest.models.Schema;
import org.apache.doris.flink.rest.models.Tablet;
import org.apache.doris.flink.sink.BackendUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_TABLET_SIZE_DEFAULT;
import static org.apache.doris.flink.cfg.ConfigurationOptions.DORIS_TABLET_SIZE_MIN;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TestRestService {
    private static final Logger logger = LoggerFactory.getLogger(TestRestService.class);

    @Rule public ExpectedException thrown = ExpectedException.none();

    private MockedStatic<BackendUtil> backendUtilMockedStatic;

    @Before
    public void setUp() throws Exception {
        backendUtilMockedStatic = mockStatic(BackendUtil.class);
        backendUtilMockedStatic.when(() -> BackendUtil.tryHttpConnection(any())).thenReturn(true);
    }

    @After
    public void after() {
        if (backendUtilMockedStatic != null) {
            backendUtilMockedStatic.close();
        }
    }

    @Test
    public void testParseIdentifier() throws Exception {
        String validIdentifier = "a.b";
        String[] names = RestService.parseIdentifier(validIdentifier, logger);
        Assert.assertEquals(2, names.length);
        Assert.assertEquals("a", names[0]);
        Assert.assertEquals("b", names[1]);

        String invalidIdentifier1 = "a";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(
                "argument 'table.identifier' is illegal, value is '" + invalidIdentifier1 + "'.");
        RestService.parseIdentifier(invalidIdentifier1, logger);
    }

    @Test
    public void testParseIdentifierIllegalnull() throws IllegalArgumentException {
        String nullIdentifier = null;
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(
                "argument 'table.identifier' is illegal, value is '" + nullIdentifier + "'.");
        RestService.parseIdentifier(nullIdentifier, logger);
    }

    @Test
    public void testParseIdentifierIllegalEmpty() throws IllegalArgumentException {
        String emptyIdentifier = "";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(
                "argument 'table.identifier' is illegal, value is '" + emptyIdentifier + "'.");
        RestService.parseIdentifier(emptyIdentifier, logger);
    }

    @Test
    public void testParseIdentifierIllegal() throws Exception {
        String invalidIdentifier3 = "a.b.c";
        RestService.parseIdentifier(invalidIdentifier3, logger);

        invalidIdentifier3 = "a.b.c.d.e";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(
                "argument 'table.identifier' is illegal, value is '" + invalidIdentifier3 + "'.");
        RestService.parseIdentifier(invalidIdentifier3, logger);
    }

    @Test
    public void testChoiceFe() throws Exception {
        String validFes = "1,2,3";
        String fe = RestService.randomEndpoint(validFes, logger);
        List<String> feNodes = new ArrayList<>(3);
        feNodes.add("1");
        feNodes.add("2");
        feNodes.add("3");
        Assert.assertTrue(feNodes.contains(fe));

        String emptyFes = "";
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("argument 'fenodes' is illegal, value is '" + emptyFes + "'.");
        RestService.randomEndpoint(emptyFes, logger);
    }

    @Test
    public void testChoiceFeError() throws Exception {
        String nullFes = null;
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("argument 'fenodes' is illegal, value is '" + nullFes + "'.");
        RestService.randomEndpoint(nullFes, logger);
    }

    @Test
    public void testFeResponseToSchema() throws Exception {
        String res =
                "{\"properties\":[{\"type\":\"TINYINT\",\"name\":\"k1\",\"comment\":\"\",\"aggregation_type\":\"\"},{\"name\":\"k5\","
                        + "\"scale\":\"0\",\"comment\":\"\",\"type\":\"DECIMALV2\",\"precision\":\"9\",\"aggregation_type\":\"\"}],\"status\":200}";
        Schema expected = new Schema();
        expected.setStatus(200);
        Field k1 = new Field("k1", "TINYINT", "", 0, 0, "");
        Field k5 = new Field("k5", "DECIMALV2", "", 9, 0, "");
        expected.put(k1);
        expected.put(k5);
        Schema actual = RestService.parseSchema(res, logger);
        Assert.assertEquals(expected.getKeysType(), actual.getKeysType());
        Assert.assertEquals(expected.getStatus(), actual.getStatus());
        Assert.assertEquals(expected.getProperties().size(), actual.getProperties().size());
        Assert.assertEquals(expected.get(0).getName(), actual.get(0).getName());
        Assert.assertEquals(expected.get(0).getType(), actual.get(0).getType());
        Assert.assertEquals(expected.get(1).getName(), actual.get(1).getName());
        Assert.assertEquals(expected.get(1).getType(), actual.get(1).getType());

        String notJsonRes = "not json";
        thrown.expect(DorisException.class);
        thrown.expectMessage(startsWith("Doris FE's response is not a json. res:"));
        RestService.parseSchema(notJsonRes, logger);
    }

    @Test
    public void testChoiceFeNo() throws Exception {
        String failFenodes = "127.0.0.1:1";
        BackendUtil.tryHttpConnection(any());
        backendUtilMockedStatic.when(() -> BackendUtil.tryHttpConnection(any())).thenReturn(false);
        thrown.expect(DorisRuntimeException.class);
        thrown.expectMessage("No Doris FE is available");
        RestService.randomEndpoint(failFenodes, logger);
    }

    @Test
    public void testFeResponseToSchemaNotMap() throws Exception {
        String notSchemaRes =
                "{\"properties_error_key\":[{\"type\":\"TINYINT\",\"name\":\"k1\",\"comment\":\"\"},"
                        + "{\"name\":\"k5\",\"scale\":\"0\",\"comment\":\"\",\"type\":\"DECIMALV2\",\"precision\":\"9\"}],"
                        + "\"status\":200}";
        Schema schema = RestService.parseSchema(notSchemaRes, logger);
        Assert.assertTrue(schema.getProperties().isEmpty());
    }

    @Test
    public void testFeResponseToSchemaNotOk() throws Exception {
        String notOkRes =
                "{\"properties\":[{\"type\":\"TINYINT\",\"name\":\"k1\",\"comment\":\"\"},{\"name\":\"k5\","
                        + "\"scale\":\"0\",\"comment\":\"\",\"type\":\"DECIMALV2\",\"precision\":\"9\"}],\"status\":20}";
        thrown.expect(DorisException.class);
        thrown.expectMessage(startsWith("Doris FE's response is not OK, status is "));
        RestService.parseSchema(notOkRes, logger);
    }

    @Test
    public void testFeResponseToSchemaShouldNotHappen() throws Exception {
        String notHappenRes = "null";
        thrown.expect(ShouldNeverHappenException.class);
        RestService.parseSchema(notHappenRes, logger);
    }

    @Test
    public void testFeResponseToQueryPlan() throws Exception {
        String res =
                "{\"partitions\":{"
                        + "\"11017\":{\"routings1\":[\"be1\",\"be2\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1},"
                        + "\"11019\":{\"routings\":[\"be3\",\"be4\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1}},"
                        + "\"opaqued_query_plan\":\"query_plan\",\"status\":200}";

        List<String> routings11017 = new ArrayList<>(2);
        routings11017.add("be1");
        routings11017.add("be2");

        Tablet tablet11017 = new Tablet();
        tablet11017.setSchemaHash(1);
        tablet11017.setVersionHash(1);
        tablet11017.setVersion(3);
        tablet11017.setRoutings(routings11017);

        List<String> routings11019 = new ArrayList<>(2);
        routings11019.add("be3");
        routings11019.add("be4");

        Tablet tablet11019 = new Tablet();
        tablet11019.setSchemaHash(1);
        tablet11019.setVersionHash(1);
        tablet11019.setVersion(3);
        tablet11019.setRoutings(routings11019);

        Map<String, Tablet> partitions = new LinkedHashMap<>();
        partitions.put("11017", tablet11017);
        partitions.put("11019", tablet11019);

        QueryPlan expected = new QueryPlan();
        expected.setPartitions(partitions);
        expected.setStatus(200);
        expected.setOpaquedQueryPlan("query_plan");

        QueryPlan actual = RestService.getQueryPlan(res, logger);
        Assert.assertEquals(expected.getOpaquedQueryPlan(), actual.getOpaquedQueryPlan());
        Assert.assertEquals(expected.getStatus(), actual.getStatus());
        Assert.assertEquals(expected.getPartitions().size(), actual.getPartitions().size());

        String notJsonRes = "{a=1}";
        thrown.expect(DorisException.class);
        thrown.expectMessage("Parse Doris FE's response to json failed");
        RestService.getQueryPlan(notJsonRes, logger);
    }

    @Test
    public void testFeResponseToQueryPlanError() throws Exception {
        String notJsonRes = "null";
        thrown.expect(ShouldNeverHappenException.class);
        RestService.getQueryPlan(notJsonRes, logger);
    }

    @Test
    public void testFeResponseToQueryPlanNotOk() throws Exception {
        String res =
                "{\"partitions\":{"
                        + "\"11017\":{\"routings\":[\"be1\",\"be2\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1},"
                        + "\"11019\":{\"routings\":[\"be3\",\"be4\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1}},"
                        + "\"opaqued_query_plan\":\"query_plan\",\"status\":400}";
        thrown.expect(DorisException.class);
        RestService.getQueryPlan(res, logger);
    }

    @Test
    public void testSelectTabletBe() throws Exception {
        String res =
                "{\"partitions\":{"
                        + "\"11017\":{\"routings\":[\"be1\",\"be2\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1},"
                        + "\"11019\":{\"routings\":[\"be3\",\"be4\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1},"
                        + "\"11021\":{\"routings\":[\"be3\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1}},"
                        + "\"opaqued_query_plan\":\"query_plan\",\"status\":200}";

        QueryPlan queryPlan = RestService.getQueryPlan(res, logger);

        List<Long> be1Tablet = new ArrayList<>();
        be1Tablet.add(11017L);
        List<Long> be3Tablet = new ArrayList<>();
        be3Tablet.add(11019L);
        be3Tablet.add(11021L);
        Map<String, List<Long>> expected = new HashMap<>();
        expected.put("be1", be1Tablet);
        expected.put("be3", be3Tablet);

        Assert.assertEquals(expected, RestService.selectBeForTablet(queryPlan, logger));

        String noBeRes =
                "{\"partitions\":{"
                        + "\"11021\":{\"routings\":[],\"version\":3,\"versionHash\":1,\"schemaHash\":1}},"
                        + "\"opaqued_query_plan\":\"query_plan\",\"status\":200}";
        thrown.expect(DorisException.class);
        thrown.expectMessage(startsWith("Cannot choice Doris BE for tablet"));
        RestService.selectBeForTablet(RestService.getQueryPlan(noBeRes, logger), logger);
    }

    @Test
    public void testSelectTabletBeError() throws DorisException {
        String notNumberRes =
                "{\"partitions\":{"
                        + "\"11021xxx\":{\"routings\":[\"be1\"],\"version\":3,\"versionHash\":1,\"schemaHash\":1}},"
                        + "\"opaqued_query_plan\":\"query_plan\",\"status\":200}";
        thrown.expect(DorisException.class);
        thrown.expectMessage(startsWith("Parse tablet id "));
        RestService.selectBeForTablet(RestService.getQueryPlan(notNumberRes, logger), logger);
    }

    @Test
    public void testGetTabletSize() {
        DorisReadOptions.Builder builder = DorisReadOptions.builder();
        Assert.assertEquals(
                DORIS_TABLET_SIZE_DEFAULT.intValue(),
                RestService.tabletCountLimitForOnePartition(builder.build(), logger));

        builder.setRequestTabletSize(null);
        Assert.assertEquals(
                DORIS_TABLET_SIZE_DEFAULT.intValue(),
                RestService.tabletCountLimitForOnePartition(builder.build(), logger));

        builder.setRequestTabletSize(10);
        Assert.assertEquals(
                10, RestService.tabletCountLimitForOnePartition(builder.build(), logger));

        builder.setRequestTabletSize(1);
        Assert.assertEquals(
                DORIS_TABLET_SIZE_MIN.intValue(),
                RestService.tabletCountLimitForOnePartition(builder.build(), logger));

        builder.setRequestTabletSize(-1);
        Assert.assertEquals(
                DORIS_TABLET_SIZE_MIN.intValue(),
                RestService.tabletCountLimitForOnePartition(builder.build(), logger));
    }

    @Test
    public void testTabletsMapToPartition() throws Exception {
        List<Long> tablets1 = new ArrayList<>();
        tablets1.add(1L);
        tablets1.add(2L);
        List<Long> tablets2 = new ArrayList<>();
        tablets2.add(3L);
        tablets2.add(4L);
        Map<String, List<Long>> beToTablets = new HashMap<>();
        beToTablets.put("be1", tablets1);
        beToTablets.put("be2", tablets2);

        String opaquedQueryPlan = "query_plan";
        String cluster = "c";
        String database = "d";
        String table = "t";

        Set<Long> be1Tablet = new HashSet<>();
        be1Tablet.add(1L);
        be1Tablet.add(2L);
        PartitionDefinition pd1 =
                new PartitionDefinition(database, table, "be1", be1Tablet, opaquedQueryPlan);

        Set<Long> be2Tablet = new HashSet<>();
        be2Tablet.add(3L);
        be2Tablet.add(4L);
        PartitionDefinition pd2 =
                new PartitionDefinition(database, table, "be2", be2Tablet, opaquedQueryPlan);

        List<PartitionDefinition> expected = new ArrayList<>();
        expected.add(pd1);
        expected.add(pd2);
        Collections.sort(expected);
        DorisOptions options = DorisOptions.builder().setFenodes("127.0.0.1:8030").build();
        DorisReadOptions readOptions = DorisReadOptions.builder().setRequestTabletSize(2).build();
        List<PartitionDefinition> actual =
                RestService.tabletsMapToPartition(
                        options,
                        readOptions,
                        beToTablets,
                        opaquedQueryPlan,
                        database,
                        table,
                        logger);
        Collections.sort(actual);

        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testParseBackendV2() throws Exception {
        String response =
                "{\"backends\":[{\"ip\":\"192.168.1.1\",\"http_port\":8042,\"is_alive\":true}, {\"ip\":\"192.168.1.2\",\"http_port\":8042,\"is_alive\":true}]}";
        List<BackendV2.BackendRowV2> backendRows = RestService.parseBackendV2(response, logger);
        Assert.assertEquals(2, backendRows.size());
        thrown.expect(ShouldNeverHappenException.class);
        response = "null";
        RestService.parseBackendV2(response, logger);
    }

    @Test
    public void testParseBackendV2Error() {
        String response =
                "{\"backends\":[{\"ip_error_key\":\"192.168.1.1\",\"http_port\":8042,\"is_alive\":true}, {\"ip\":\"192.168.1.2\",\"http_port\":8042,\"is_alive\":true}]}";
        List<BackendV2.BackendRowV2> backendRowV2s = RestService.parseBackendV2(response, logger);
        Assert.assertEquals(2, backendRowV2s.size());
        List<String> actual = backendRowV2s.stream().map(m -> m.ip).collect(Collectors.toList());
        List<String> excepted = Arrays.asList(null, "192.168.1.2");
        Assert.assertEquals(actual.size(), excepted.size());
        Assert.assertTrue(actual.containsAll(excepted));
    }

    @Test
    public void testGetBackendsV2() {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:1,127.0.0.1:2")
                        .setAutoRedirect(false)
                        .build();
        DorisReadOptions readOptions = DorisReadOptions.defaults();
        thrown.expect(DorisRuntimeException.class);
        thrown.expectMessage(startsWith("No Doris FE is available"));
        RestService.getBackendsV2(options, readOptions, logger);
    }

    @Test
    public void testAllEndpoints() {
        thrown.expect(DorisRuntimeException.class);
        thrown.expectMessage(startsWith("fenodes is empty"));
        RestService.allEndpoints("", logger);
    }

    @Test
    public void testParseResponse() throws IOException {
        HttpURLConnection connection = mock(HttpURLConnection.class);
        when(connection.getResponseCode()).thenReturn(100);
        thrown.expect(IOException.class);
        thrown.expectMessage(startsWith("Failed to get response from Doris"));
        RestService.parseResponse(connection, logger);
    }

    @Test
    public void testUniqueKeyType() throws IOException {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:1,127.0.0.1:2")
                        .setTableIdentifier("db.tbl")
                        .setAutoRedirect(false)
                        .build();
        DorisReadOptions readOptions = DorisReadOptions.defaults();
        thrown.expect(DorisRuntimeException.class);
        RestService.isUniqueKeyType(options, readOptions, logger);
    }
}
