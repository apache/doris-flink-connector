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

package org.apache.doris.flink.sink.overwrite;

import java.io.Serializable;
import java.util.Objects;

/** Doris table identifier for overwrite DDL. */
public class DorisTableIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String database;
    private final String table;

    public DorisTableIdentifier(String database, String table) {
        if (database == null || database.trim().isEmpty()) {
            throw new IllegalArgumentException("Doris database name must not be empty.");
        }
        if (table == null || table.trim().isEmpty()) {
            throw new IllegalArgumentException("Doris table name must not be empty.");
        }
        this.database = database.trim();
        this.table = table.trim();
    }

    public static DorisTableIdentifier of(String tableIdentifier) {
        if (tableIdentifier == null || tableIdentifier.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "table.identifier is required for INSERT OVERWRITE.");
        }
        String[] parts = tableIdentifier.trim().split("\\.", -1);
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "INSERT OVERWRITE only supports single-table identifier db.table.");
        }
        return new DorisTableIdentifier(parts[0], parts[1]);
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String asString() {
        return database + "." + table;
    }

    public String toSql() {
        return quote(database) + "." + quote(table);
    }

    public static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    @Override
    public String toString() {
        return asString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisTableIdentifier that = (DorisTableIdentifier) o;
        return Objects.equals(database, that.database) && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, table);
    }
}
