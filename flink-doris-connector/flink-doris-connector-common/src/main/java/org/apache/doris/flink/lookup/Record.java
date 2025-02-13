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

import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.Arrays;

/** record. */
public class Record implements Serializable {

    LookupSchema schema;
    Object[] values;

    public Record(LookupSchema schema) {
        this.schema = schema;
        values = new Object[schema.getFieldTypes().length];
    }

    public LookupSchema getSchema() {
        return schema;
    }

    public Object getObject(int index) {
        return values[index];
    }

    public void setObject(int index, Object obj) {
        values[index] = obj;
    }

    public String getTableIdentifier() {
        return schema.getTableIdentifier();
    }

    public String[] getSelectFields() {
        return schema.getSelectFields();
    }

    public String[] getConditionFields() {
        return schema.getConditionFields();
    }

    public DataType[] getFieldTypes() {
        return schema.getFieldTypes();
    }

    public int[] getKeyIndex() {
        return schema.getKeyIndex();
    }

    public Object[] getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "Record{" + "schema=" + schema + ", values=" + Arrays.toString(values) + '}';
    }
}
