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

import org.apache.doris.flink.rest.PartitionDefinition;

import java.util.Objects;

/** A bounded snapshot split backed by a Doris tablet partition definition. */
public final class DorisSnapshotSplit implements DorisSourceSplit {
    private final String splitId;
    private final PartitionDefinition partitionDefinition;

    public DorisSnapshotSplit(String splitId, PartitionDefinition partitionDefinition) {
        this.splitId = Objects.requireNonNull(splitId, "splitId");
        this.partitionDefinition =
                Objects.requireNonNull(partitionDefinition, "partitionDefinition");
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public PartitionDefinition getPartitionDefinition() {
        return partitionDefinition;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof DorisSnapshotSplit)) {
            return false;
        }
        DorisSnapshotSplit that = (DorisSnapshotSplit) object;
        return partitionDefinition.equals(that.partitionDefinition);
    }

    @Override
    public int hashCode() {
        return partitionDefinition.hashCode();
    }

    @Override
    public String toString() {
        return String.format(
                "DorisSnapshotSplit: database=%s,table=%s,id=%s,be=%s,tablets=%s",
                partitionDefinition.getDatabase(),
                partitionDefinition.getTable(),
                splitId,
                partitionDefinition.getBeAddress(),
                partitionDefinition.getTabletIds());
    }
}
