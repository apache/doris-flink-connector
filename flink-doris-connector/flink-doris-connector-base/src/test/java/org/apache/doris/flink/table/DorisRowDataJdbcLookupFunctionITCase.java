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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;

import com.google.common.cache.Cache;
import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.container.AbstractITCaseService;
import org.apache.doris.flink.container.ContainerUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class DorisRowDataJdbcLookupFunctionITCase extends AbstractITCaseService {
    private static final Logger LOG =
            LoggerFactory.getLogger(DorisRowDataJdbcLookupFunctionITCase.class);

    private static final String LOOKUP_TABLE = "test.t_lookup_table";

    private static final String[] fieldNames = new String[] {"id1", "id2", "c_string", "c_double"};
    private static final DataType[] fieldDataTypes =
            new DataType[] {
                DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.DOUBLE()
            };
    private static final String[] lookupKeys = new String[] {"id1", "id2"};
    private static final int[] keyIndexs = new int[] {0, 1};

    @Before
    public void setUp() throws Exception {
        ContainerUtils.executeSQLStatement(
                getDorisQueryConnection(),
                LOG,
                String.format("CREATE DATABASE IF NOT EXISTS %s", "test"),
                String.format("DROP TABLE IF EXISTS %s", LOOKUP_TABLE),
                String.format(
                        "CREATE TABLE %s ( \n"
                                + "`id1` int,\n"
                                + "`id2` varchar(128),\n"
                                + "`c_string` string,\n"
                                + "`c_double` double\n"
                                + ") DISTRIBUTED BY HASH(`id1`) BUCKETS 1\n"
                                + "PROPERTIES (\n"
                                + "\"replication_num\" = \"1\"\n"
                                + ")\n",
                        LOOKUP_TABLE),
                String.format(
                        "insert into %s  values (1,'A','zhangsanA',1.12),"
                                + "(1,'A','zhangsanA-1',11.12),"
                                + "(2,'B','zhangsanB',2.12),(4,'D','zhangsanD',4.12)",
                        LOOKUP_TABLE));
    }

    @Test
    public void testEval() throws Exception {
        DorisLookupOptions lookupOptions =
                DorisLookupOptions.builder()
                        .setJdbcReadBatchQueueSize(16)
                        .setJdbcReadBatchSize(16)
                        .setJdbcReadThreadSize(1)
                        .setMaxRetryTimes(1)
                        .build();
        DorisRowDataJdbcLookupFunction lookupFunction =
                buildRowDataJdbcLookupFunction(lookupOptions);

        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);

        FunctionContext context = new FunctionContext(null);
        lookupFunction.open(context);

        lookupFunction.eval(1, StringData.fromString("A"));
        lookupFunction.eval(2, StringData.fromString("B"));

        List<String> result =
                new ArrayList<>(collector.getOutputs())
                        .stream().map(RowData::toString).sorted().collect(Collectors.toList());

        List<String> expected = new ArrayList<>();
        expected.add("+I(1,A,zhangsanA,1.12)");
        expected.add("+I(1,A,zhangsanA-1,11.12)");
        expected.add("+I(2,B,zhangsanB,2.12)");
        Collections.sort(expected);

        assertEquals(expected, result);
    }

    @Test
    public void testEvalWithCache() throws Exception {
        long cacheExpireMs = 20000;
        DorisLookupOptions lookupOptions =
                DorisLookupOptions.builder()
                        .setCacheExpireMs(cacheExpireMs)
                        .setCacheMaxSize(10)
                        .setJdbcReadBatchQueueSize(16)
                        .setJdbcReadBatchSize(16)
                        .setJdbcReadThreadSize(1)
                        .setMaxRetryTimes(1)
                        .build();

        DorisRowDataJdbcLookupFunction lookupFunction =
                buildRowDataJdbcLookupFunction(lookupOptions);

        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);

        FunctionContext context = new FunctionContext(null);
        lookupFunction.open(context);

        lookupFunction.eval(4, StringData.fromString("D"));
        lookupFunction.eval(5, StringData.fromString("5"));
        RowData keyRow = GenericRowData.of(4, StringData.fromString("D"));
        RowData keyRowNoExist = GenericRowData.of(5, StringData.fromString("5"));
        Cache<RowData, List<RowData>> cache = lookupFunction.getCache();
        // empty data should cache
        assertEquals(
                cache.getIfPresent(keyRow),
                Arrays.asList(
                        GenericRowData.of(
                                4,
                                StringData.fromString("D"),
                                StringData.fromString("zhangsanD"),
                                4.12)));
        assertEquals(cache.getIfPresent(keyRowNoExist), Collections.<RowData>emptyList());

        // cache data expire
        Thread.sleep(cacheExpireMs);
        assert cache.getIfPresent(keyRow) == null;
    }

    private DorisRowDataJdbcLookupFunction buildRowDataJdbcLookupFunction(
            DorisLookupOptions lookupOptions) {
        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes(getFenodes())
                        .setTableIdentifier(LOOKUP_TABLE)
                        .setJdbcUrl(getDorisQueryUrl())
                        .setUsername(getDorisUsername())
                        .setPassword(getDorisPassword())
                        .build();

        DorisRowDataJdbcLookupFunction lookupFunction =
                new DorisRowDataJdbcLookupFunction(
                        dorisOptions,
                        lookupOptions,
                        fieldNames,
                        fieldDataTypes,
                        lookupKeys,
                        keyIndexs);

        return lookupFunction;
    }

    private static final class ListOutputCollector implements Collector<RowData> {

        private final List<RowData> output = new ArrayList<>();

        @Override
        public void collect(RowData row) {
            this.output.add(row);
        }

        @Override
        public void close() {}

        public List<RowData> getOutputs() {
            return output;
        }
    }
}
