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

package org.apache.doris.flink.sink.writer.tvf;

import org.apache.doris.flink.sink.DorisAbstractCommittable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Metadata needed to load staged S3 objects for one table and checkpoint. */
public class S3TvfCommittable implements DorisAbstractCommittable {

    private final long checkpointId;
    private final String database;
    private final String table;
    private final String label;
    private final List<String> objectKeys;
    private final List<String> columns;
    private final boolean deleteSignEnabled;

    public S3TvfCommittable(
            long checkpointId,
            String database,
            String table,
            String label,
            List<String> objectKeys,
            List<String> columns,
            boolean deleteSignEnabled) {
        this.checkpointId = checkpointId;
        this.database = database;
        this.table = table;
        this.label = label;
        this.objectKeys = immutableCopy(objectKeys);
        this.columns = immutableCopy(columns);
        this.deleteSignEnabled = deleteSignEnabled;
    }

    private static List<String> immutableCopy(List<String> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getLabel() {
        return label;
    }

    public List<String> getObjectKeys() {
        return objectKeys;
    }

    public List<String> getColumns() {
        return columns;
    }

    public boolean isDeleteSignEnabled() {
        return deleteSignEnabled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3TvfCommittable that = (S3TvfCommittable) o;
        return checkpointId == that.checkpointId
                && deleteSignEnabled == that.deleteSignEnabled
                && Objects.equals(database, that.database)
                && Objects.equals(table, that.table)
                && Objects.equals(label, that.label)
                && Objects.equals(objectKeys, that.objectKeys)
                && Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                checkpointId, database, table, label, objectKeys, columns, deleteSignEnabled);
    }

    @Override
    public String toString() {
        return "S3TvfCommittable{"
                + "checkpointId="
                + checkpointId
                + ", database='"
                + database
                + '\''
                + ", table='"
                + table
                + '\''
                + ", label='"
                + label
                + '\''
                + ", objectKeys="
                + objectKeys
                + ", columns="
                + columns
                + ", deleteSignEnabled="
                + deleteSignEnabled
                + '}';
    }
}
