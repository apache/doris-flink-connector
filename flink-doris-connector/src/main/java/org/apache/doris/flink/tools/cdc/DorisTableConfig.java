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

package org.apache.doris.flink.tools.cdc;

import org.apache.flink.annotation.VisibleForTesting;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class DorisTableConfig implements Serializable {
    private static final String LIGHT_SCHEMA_CHANGE = "light_schema_change";
    // PROPERTIES parameter in doris table creation statement. such as: replication_num=1.
    private final Map<String, String> tableProperties;
    // The specific parameters extracted from --table-conf need to be parsed and integrated into the
    // doris table creation statement. such as: table-buckets="tbl1:10,tbl2:20,a.*:30,b.*:40,.*:50".
    private Map<String, Integer> tableBuckets;

    // Only for testing
    @VisibleForTesting
    public DorisTableConfig() {
        tableProperties = new HashMap<>();
        tableBuckets = new HashMap<>();
    }

    public DorisTableConfig(Map<String, String> tableConfig) {
        if (Objects.isNull(tableConfig)) {
            tableConfig = new HashMap<>();
        }
        // default enable light schema change
        if (!tableConfig.containsKey(LIGHT_SCHEMA_CHANGE)) {
            tableConfig.put(LIGHT_SCHEMA_CHANGE, Boolean.toString(true));
        }
        if (tableConfig.containsKey(DatabaseSyncConfig.TABLE_BUCKETS)) {
            this.tableBuckets =
                    buildTableBucketMap(tableConfig.get(DatabaseSyncConfig.TABLE_BUCKETS));
            tableConfig.remove(DatabaseSyncConfig.TABLE_BUCKETS);
        }
        tableProperties = tableConfig;
    }

    public Map<String, Integer> getTableBuckets() {
        return tableBuckets;
    }

    public Map<String, String> getTableProperties() {
        return tableProperties;
    }

    /**
     * Build table bucket Map.
     *
     * @param tableBuckets the string of tableBuckets, eg:student:10,student_info:20,student.*:30
     * @return The table name and buckets map. The key is table name, the value is buckets.
     */
    @VisibleForTesting
    public Map<String, Integer> buildTableBucketMap(String tableBuckets) {
        Map<String, Integer> tableBucketsMap = new LinkedHashMap<>();
        String[] tableBucketsArray = tableBuckets.split(",");
        for (String tableBucket : tableBucketsArray) {
            String[] tableBucketArray = tableBucket.split(":");
            tableBucketsMap.put(
                    tableBucketArray[0].trim(), Integer.parseInt(tableBucketArray[1].trim()));
        }
        return tableBucketsMap;
    }
}
