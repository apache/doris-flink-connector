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

package org.apache.doris.flink.source.split;

import java.util.Objects;
import java.util.regex.Pattern;

/** A finite Doris row-binlog query range with an exclusive start and inclusive end. */
public final class DorisStreamSplit implements DorisSourceSplit {
    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");

    private final String splitId;
    private final String startTimestamp;
    private final String endTimestamp;

    public DorisStreamSplit(String splitId, String startTimestamp, String endTimestamp) {
        validateTimestamp(startTimestamp);
        validateTimestamp(endTimestamp);
        if (startTimestamp.compareTo(endTimestamp) >= 0) {
            throw new IllegalArgumentException(
                    "Stream split start timestamp must be before end timestamp");
        }
        this.splitId = Objects.requireNonNull(splitId, "splitId");
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    public static DorisStreamSplit of(String startTimestamp, String endTimestamp) {
        return new DorisStreamSplit(
                createSplitId(startTimestamp, endTimestamp), startTimestamp, endTimestamp);
    }

    public static boolean isValidTimestamp(String value) {
        return value != null && TIMESTAMP_PATTERN.matcher(value).matches();
    }

    private static void validateTimestamp(String value) {
        if (!isValidTimestamp(value)) {
            throw new IllegalArgumentException(
                    "Timestamp must strictly match yyyy-MM-dd HH:mm:ss: " + value);
        }
    }

    private static String createSplitId(String startTimestamp, String endTimestamp) {
        validateTimestamp(startTimestamp);
        validateTimestamp(endTimestamp);
        return "stream-" + compact(startTimestamp) + "-" + compact(endTimestamp);
    }

    private static String compact(String timestamp) {
        return timestamp.replace("-", "").replace(" ", "T").replace(":", "");
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public String getStartTimestamp() {
        return startTimestamp;
    }

    public String getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof DorisStreamSplit)) {
            return false;
        }
        DorisStreamSplit that = (DorisStreamSplit) object;
        return splitId.equals(that.splitId)
                && startTimestamp.equals(that.startTimestamp)
                && endTimestamp.equals(that.endTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, startTimestamp, endTimestamp);
    }

    @Override
    public String toString() {
        return "DorisStreamSplit{" + splitId + ", (" + startTimestamp + ", " + endTimestamp + "]}";
    }
}
