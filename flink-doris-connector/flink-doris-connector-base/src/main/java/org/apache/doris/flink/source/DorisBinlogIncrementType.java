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

package org.apache.doris.flink.source;

import java.util.Locale;

/** Increment semantics requested from the Doris row-binlog query. */
public enum DorisBinlogIncrementType {
    DETAIL,
    MIN_DELTA,
    APPEND_ONLY;

    public static DorisBinlogIncrementType fromOption(String value) {
        if (value == null) {
            throw new IllegalArgumentException("source.binlog.increment-type must not be null");
        }
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (java.lang.IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unsupported source.binlog.increment-type: " + value, e);
        }
    }

    public String toSqlValue() {
        return name();
    }
}
