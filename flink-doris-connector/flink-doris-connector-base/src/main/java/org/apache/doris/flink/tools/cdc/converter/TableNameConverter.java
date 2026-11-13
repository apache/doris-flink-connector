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

package org.apache.doris.flink.tools.cdc.converter;

import java.io.Serializable;
import java.util.Map;
import java.util.regex.Pattern;

/*
 * Convert the table name of the upstream data source to the table name of the doris database.
 * */
public class TableNameConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String prefix;
    private final String suffix;

    // tbl_.*, tbl
    private Map<Pattern, String> routeRules;

    public TableNameConverter() {
        this("", "");
    }

    public TableNameConverter(String prefix, String suffix) {
        this.prefix = prefix == null ? "" : prefix;
        this.suffix = suffix == null ? "" : suffix;
    }

    public TableNameConverter(String prefix, String suffix, Map<Pattern, String> routeRules) {
        this.prefix = prefix == null ? "" : prefix;
        this.suffix = suffix == null ? "" : suffix;
        this.routeRules = routeRules;
    }

    public String convert(String tableName) {
        if (routeRules == null) {
            return prefix + tableName + suffix;
        }

        String target = null;

        for (Map.Entry<Pattern, String> patternStringEntry : routeRules.entrySet()) {
            if (patternStringEntry.getKey().matcher(tableName).matches()) {
                target = patternStringEntry.getValue();
            }
        }
        /**
         * If routeRules is not null and target is not assigned, then the synchronization task
         * contains both multi to one and one to one , prefixes and suffixes are added to common
         * one-to-one mapping tables
         */
        if (target == null) {
            return prefix + tableName + suffix;
        }
        return target;
    }
}
