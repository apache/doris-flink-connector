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

import org.apache.doris.flink.cfg.DorisOptions;

import java.io.Serializable;
import java.util.Objects;

/** Metadata required to finalize an INSERT OVERWRITE staging write. */
public class DorisOverwriteOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final DorisOptions targetOptions;
    private final DorisTableIdentifier targetTable;
    private final DorisTableIdentifier stagingTable;
    private final Long targetTableId;
    private final Long stagingTableId;
    private final String attemptId;

    public DorisOverwriteOptions(
            DorisOptions targetOptions,
            DorisTableIdentifier targetTable,
            DorisTableIdentifier stagingTable,
            Long targetTableId,
            Long stagingTableId,
            String attemptId) {
        this.targetOptions =
                DorisOverwriteManager.copyOptions(targetOptions, targetTable.asString());
        this.targetTable = targetTable;
        this.stagingTable = stagingTable;
        this.targetTableId = targetTableId;
        this.stagingTableId = stagingTableId;
        this.attemptId = attemptId;
    }

    public DorisOptions getTargetOptions() {
        return DorisOverwriteManager.copyOptions(targetOptions, targetTable.asString());
    }

    public DorisTableIdentifier getTargetTable() {
        return targetTable;
    }

    public DorisTableIdentifier getStagingTable() {
        return stagingTable;
    }

    public Long getTargetTableId() {
        return targetTableId;
    }

    public Long getStagingTableId() {
        return stagingTableId;
    }

    public String getAttemptId() {
        return attemptId;
    }

    @Override
    public String toString() {
        return "DorisOverwriteOptions{"
                + "targetTable="
                + targetTable
                + ", stagingTable="
                + stagingTable
                + ", targetTableId="
                + targetTableId
                + ", stagingTableId="
                + stagingTableId
                + ", attemptId='"
                + attemptId
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisOverwriteOptions that = (DorisOverwriteOptions) o;
        return Objects.equals(targetOptions, that.targetOptions)
                && Objects.equals(targetTable, that.targetTable)
                && Objects.equals(stagingTable, that.stagingTable)
                && Objects.equals(targetTableId, that.targetTableId)
                && Objects.equals(stagingTableId, that.stagingTableId)
                && Objects.equals(attemptId, that.attemptId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                targetOptions, targetTable, stagingTable, targetTableId, stagingTableId, attemptId);
    }
}
