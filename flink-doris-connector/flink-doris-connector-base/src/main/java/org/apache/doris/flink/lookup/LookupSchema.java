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

public class LookupSchema implements Serializable {

    private String tableIdentifier;
    private String[] selectFields;
    private String[] conditionFields;
    private DataType[] fieldTypes;
    private int[] keyIndex;

    public LookupSchema(
            String tableIdentifier,
            String[] selectFields,
            DataType[] fieldTypes,
            String[] conditionFields,
            int[] keyIndex) {
        this.tableIdentifier = tableIdentifier;
        this.selectFields = selectFields;
        this.fieldTypes = fieldTypes;
        this.conditionFields = conditionFields;
        this.keyIndex = keyIndex;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public String[] getSelectFields() {
        return selectFields;
    }

    public String[] getConditionFields() {
        return conditionFields;
    }

    public DataType[] getFieldTypes() {
        return fieldTypes;
    }

    public int[] getKeyIndex() {
        return keyIndex;
    }

    public void setTableIdentifier(String tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
    }

    public void setSelectFields(String[] selectFields) {
        this.selectFields = selectFields;
    }

    public void setConditionFields(String[] conditionFields) {
        this.conditionFields = conditionFields;
    }

    public void setFieldTypes(DataType[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public void setKeyIndex(int[] keyIndex) {
        this.keyIndex = keyIndex;
    }

    @Override
    public String toString() {
        return "LookupSchema{"
                + "tableIdentifier='"
                + tableIdentifier
                + '\''
                + ", selectFields="
                + Arrays.toString(selectFields)
                + ", conditionFields="
                + Arrays.toString(conditionFields)
                + ", fieldTypes="
                + Arrays.toString(fieldTypes)
                + ", keyIndex="
                + Arrays.toString(keyIndex)
                + '}';
    }
}
