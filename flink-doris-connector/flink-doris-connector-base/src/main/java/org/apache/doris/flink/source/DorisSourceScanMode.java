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

/** Defines how a Doris source starts and whether it continues with incremental reads. */
public enum DorisSourceScanMode {
    SNAPSHOT("snapshot"),
    INITIAL("initial"),
    LATEST("latest"),
    FROM_TIMESTAMP("from-timestamp");

    private final String optionValue;

    DorisSourceScanMode(String optionValue) {
        this.optionValue = optionValue;
    }

    public static DorisSourceScanMode fromOption(String value) {
        if (value == null) {
            throw new IllegalArgumentException("source.scan.mode must not be null");
        }
        String normalizedValue = value.trim();
        for (DorisSourceScanMode scanMode : values()) {
            if (scanMode.optionValue.equalsIgnoreCase(normalizedValue)) {
                return scanMode;
            }
        }
        throw new IllegalArgumentException("Unsupported source.scan.mode: " + value);
    }

    public boolean hasIncrementalPhase() {
        return this != SNAPSHOT;
    }

    public boolean isSnapshotOnly() {
        return this == SNAPSHOT;
    }

    public boolean isSnapshotPhaseRequired() {
        return this == SNAPSHOT || this == INITIAL;
    }
}
