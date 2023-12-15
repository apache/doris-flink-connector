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

package org.apache.doris.flink.lookup;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordTest {

    private LookupSchema schema;

    @Before
    public void before() {
        String tableIdentifier = "db.tbl";
        String[] selectFields = new String[] {"a", "b", "c"};
        DataType[] fieldTypes =
                new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()};
        schema = new LookupSchema(tableIdentifier, selectFields, fieldTypes, null, null);
    }

    @Test
    public void testLookupOneKey() {
        String[] conditionFields = new String[] {"a"};
        int[] keyIndex = new int[] {1};
        Object[] values = new Object[schema.getFieldTypes().length];
        values[0] = 1001;
        Record record = appendValues(conditionFields, keyIndex, values);

        Map<RecordKey, Record> map = new HashMap<>();
        map.put(new RecordKey(record), record);
        Assert.assertTrue(map.get(new RecordKey(record)) != null);
    }

    @Test
    public void testLookupTwoKey() {
        String[] conditionFields = new String[] {"a", "b"};
        int[] keyIndex = new int[] {1, 2};
        Object[] values = new Object[schema.getFieldTypes().length];
        values[0] = 1001;
        values[1] = "doris";
        Record record = appendValues(conditionFields, keyIndex, values);

        Map<RecordKey, Record> map = new HashMap<>();
        map.put(new RecordKey(record), record);
        Assert.assertTrue(map.get(new RecordKey(record)) != null);
    }

    @Test
    public void testLookupOnlyTwoKey() {
        String[] conditionFields = new String[] {"b"};
        int[] keyIndex = new int[] {2};
        Object[] values = new Object[schema.getFieldTypes().length];
        values[0] = "doris";
        Record record = appendValues(conditionFields, keyIndex, values);

        Map<RecordKey, Record> map = new HashMap<>();
        map.put(new RecordKey(record), record);
        Assert.assertTrue(map.get(new RecordKey(record)) != null);
    }

    @Test
    public void testDeduplicateRecords() {
        String[] conditionFields = new String[] {"b", "c"};
        int[] keyIndex = new int[] {2};
        Object[] values = new Object[schema.getFieldTypes().length];
        values[0] = "doris";
        values[1] = "18";
        Record record = appendValues(conditionFields, keyIndex, values);

        Object[] values2 = new Object[schema.getFieldTypes().length];
        values2[0] = "doris";
        values2[1] = "18";
        Record record2 = appendValues(conditionFields, keyIndex, values2);

        Object[] values3 = new Object[schema.getFieldTypes().length];
        values3[0] = "doris";
        values3[1] = "1";
        Record record3 = appendValues(conditionFields, keyIndex, values3);

        List<Get> list = new ArrayList<>();
        list.add(new Get(record));
        list.add(new Get(record2));
        list.add(new Get(record3));

        List<Get> gets = Worker.deduplicateRecords(list);
        Assert.assertTrue(gets.size() == 2);
    }

    private Record appendValues(String[] conditionFields, int[] keyIndex, Object[] values) {
        schema.setKeyIndex(keyIndex);
        schema.setConditionFields(conditionFields);
        Record record = new Record(schema);
        for (int i = 0; i < schema.getFieldTypes().length; i++) {
            record.setObject(i, values[i]);
        }
        return record;
    }
}
