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
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;

import com.google.common.cache.Cache;
import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@Ignore
public class DorisRowDataLookupFunctionTest {

    private static final String TEST_FENODES = "127.0.0.1:8030";
    private static final String LOOKUP_TABLE = "test.t_lookup_table";

    private static String[] fieldNames = new String[] {"id1", "id2", "c_string", "c_double"};
    private static DataType[] fieldDataTypes =
            new DataType[] {
                DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.DOUBLE()
            };
    private static String[] lookupKeys = new String[] {"id1", "id2"};

    @Test
    public void testEval() throws Exception {

        DorisLookupOptions lookupOptions = DorisLookupOptions.builder().build();
        DorisRowDataLookupFunction lookupFunction = buildRowDataLookupFunction(lookupOptions);

        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);

        lookupFunction.open(null);

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
        long cacheExpireMs = 10000;
        DorisLookupOptions lookupOptions =
                DorisLookupOptions.builder()
                        .setCacheExpireMs(cacheExpireMs)
                        .setCacheMaxSize(10)
                        .build();

        DorisRowDataLookupFunction lookupFunction = buildRowDataLookupFunction(lookupOptions);

        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);

        lookupFunction.open(null);

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

    private DorisRowDataLookupFunction buildRowDataLookupFunction(
            DorisLookupOptions lookupOptions) {
        DorisOptions dorisOptions =
                DorisOptions.builder()
                        .setFenodes(TEST_FENODES)
                        .setTableIdentifier(LOOKUP_TABLE)
                        .setUsername("root")
                        .setPassword("")
                        .build();

        DorisReadOptions readOptions = DorisReadOptions.builder().build();

        DorisRowDataLookupFunction lookupFunction =
                new DorisRowDataLookupFunction(
                        dorisOptions,
                        readOptions,
                        lookupOptions,
                        fieldNames,
                        fieldDataTypes,
                        lookupKeys);

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
