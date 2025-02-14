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

package org.apache.doris.flink.deserialization;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.doris.flink.utils.FactoryMocks.PHYSICAL_TYPE;
import static org.junit.Assert.assertEquals;

public class RowDataDeserializationSchemaTest {

    @Test
    public void deserializeTest() throws Exception {
        String[][] records = {{"flink", "1", "true"}, {"doris", "2", "false"}};
        SimpleCollector collector = new SimpleCollector();
        RowDataDeserializationSchema deserializationSchema =
                new RowDataDeserializationSchema(PHYSICAL_TYPE);
        for (String[] record : records) {
            deserializationSchema.deserialize(Arrays.asList(record), collector);
        }

        List<String> expected = Arrays.asList("+I(flink,1,true)", "+I(doris,2,false)");

        List<String> actual =
                collector.list.stream().map(Object::toString).collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    private static class SimpleCollector implements Collector<RowData> {
        private List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
