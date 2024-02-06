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

package org.apache.doris.flink.tools.cdc.mongodb;

import org.apache.doris.flink.catalog.doris.DorisType;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class MongoDBSchema extends SourceSchema {

    public MongoDBSchema(
            //            HashMap<String, Object> fieldDatas,
            ArrayList<Document> sampleDatas,
            String databaseName,
            String tableName,
            String tableComment)
            throws Exception {
        super(databaseName, null, tableName, tableComment);
        fields = new LinkedHashMap<>();
        for (Document sampleData : sampleDatas) {
            processSampleData(sampleData);
        }

        primaryKeys = new ArrayList<>();
        primaryKeys.add("_id");
    }

    private void processSampleData(Document sampleData) {
        for (String fieldName : sampleData.keySet()) {
            Object value = sampleData.get(fieldName);
            String dorisType = MongoDBType.toDorisType(value);
            if (isDecimalField(fieldName)) {
                dorisType = replaceDecimalTypeIfNeeded(fieldName, dorisType);
            }
            fields.put(fieldName, new FieldSchema(fieldName, dorisType, null));
        }
    }

    private boolean isDecimalField(String fieldName) {
        FieldSchema existingField = fields.get(fieldName);
        return existingField != null && existingField.getTypeString().startsWith(DorisType.DECIMAL);
    }

    private String replaceDecimalTypeIfNeeded(String fieldName, String newDorisType) {
        FieldSchema existingField = fields.get(fieldName);
        if (existingField.getTypeString().startsWith(DorisType.DECIMAL)) {
            int[] existingPrecisionAndScale =
                    MongoDBType.getDecimalPrecisionAndScale(existingField.getTypeString());
            int existingPrecision = existingPrecisionAndScale[0];
            int existingScale = existingPrecisionAndScale[1];

            // 提取当前值的 decimal 精度和小数位数
            int[] currentPrecisionAndScale = MongoDBType.getDecimalPrecisionAndScale(newDorisType);
            int currentPrecision = currentPrecisionAndScale[0];
            int currentScale = currentPrecisionAndScale[1];

            // 选择更大的精度进行替换
            int newPrecision = Math.max(existingPrecision, currentPrecision);
            int newScale = Math.max(existingScale, currentScale);

            return DorisType.DECIMAL + "(" + newPrecision + "," + newScale + ")";
        }
        return newDorisType;
    }
}
