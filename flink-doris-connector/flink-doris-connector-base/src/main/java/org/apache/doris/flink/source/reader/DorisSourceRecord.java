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

package org.apache.doris.flink.source.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.RowKind;

import org.apache.doris.flink.exception.DorisRuntimeException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** A single Doris row and its row-binlog metadata. */
@PublicEvolving
public final class DorisSourceRecord {
    private final List<?> fieldValues;
    private final RowKind rowKind;
    private final Long binlogTso;
    private final Long binlogLsn;
    private final Long binlogOp;

    public DorisSourceRecord(
            List<?> fieldValues, RowKind rowKind, Long binlogTso, Long binlogLsn, Long binlogOp) {
        this.fieldValues = Collections.unmodifiableList(new ArrayList<Object>(fieldValues));
        this.rowKind = Objects.requireNonNull(rowKind, "rowKind");
        this.binlogTso = binlogTso;
        this.binlogLsn = binlogLsn;
        this.binlogOp = binlogOp;
    }

    public static DorisSourceRecord snapshot(List<?> fieldValues) {
        return new DorisSourceRecord(fieldValues, RowKind.INSERT, null, null, null);
    }

    public static DorisSourceRecord incremental(List<?> fieldValues, Long tso, Long lsn, Long op) {
        if (tso == null || lsn == null || op == null) {
            throw new DorisRuntimeException("Doris row-binlog metadata cannot be null");
        }
        return new DorisSourceRecord(fieldValues, toRowKind(op), tso, lsn, op);
    }

    private static RowKind toRowKind(long op) {
        if (op == 0L) {
            return RowKind.INSERT;
        }
        if (op == 1L) {
            return RowKind.DELETE;
        }
        if (op == 2L) {
            return RowKind.UPDATE_BEFORE;
        }
        if (op == 3L) {
            return RowKind.UPDATE_AFTER;
        }
        throw new DorisRuntimeException("Unsupported Doris binlog operation: " + op);
    }

    public List<?> getFieldValues() {
        return fieldValues;
    }

    public RowKind getRowKind() {
        return rowKind;
    }

    public Long getBinlogTso() {
        return binlogTso;
    }

    public Long getBinlogLsn() {
        return binlogLsn;
    }

    public Long getBinlogOp() {
        return binlogOp;
    }
}
