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

package org.apache.doris.flink.sink.writer;

import java.util.Objects;

/** hold state for DorisWriter. */
public class DorisWriterState {
    String labelPrefix;
    String database;
    String table;
    int subtaskId;

    public DorisWriterState(String labelPrefix) {
        this.labelPrefix = labelPrefix;
    }

    public DorisWriterState(String labelPrefix, String database, String table, int subtaskId) {
        this.labelPrefix = labelPrefix;
        this.database = database;
        this.table = table;
        this.subtaskId = subtaskId;
    }

    public String getLabelPrefix() {
        return labelPrefix;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisWriterState that = (DorisWriterState) o;
        return Objects.equals(labelPrefix, that.labelPrefix)
                && Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && Objects.equals(subtaskId, that.subtaskId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(labelPrefix, database, table, subtaskId);
    }

    @Override
    public String toString() {
        return "DorisWriterState{"
                + "labelPrefix='"
                + labelPrefix
                + '\''
                + ", database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", subtaskId="
                + subtaskId
                + '}';
    }
}
