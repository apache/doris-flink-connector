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

import org.apache.flink.api.connector.source.SourceSplit;

import org.apache.doris.flink.rest.PartitionDefinition;

import javax.annotation.Nullable;

import java.util.Objects;

/** A {@link SourceSplit} that represents a {@link PartitionDefinition}. */
public class DorisSourceSplit implements SourceSplit {
    private String id;
    private final PartitionDefinition partitionDefinition;

    /**
     * The splits are frequently serialized into checkpoints. Caching the byte representation makes
     * repeated serialization cheap. This field is used by {@link DorisSourceSplitSerializer}.
     */
    @Nullable transient byte[] serializedFormCache;

    public DorisSourceSplit(String id, PartitionDefinition partitionDefinition) {
        this.id = id;
        this.partitionDefinition = partitionDefinition;
    }

    @Override
    public String splitId() {
        return id;
    }

    public PartitionDefinition getPartitionDefinition() {
        return partitionDefinition;
    }

    @Override
    public String toString() {
        return String.format(
                "DorisSourceSplit: %s.%s,id=%s,be=%s,tablets=%s",
                partitionDefinition.getDatabase(),
                partitionDefinition.getTable(),
                id,
                partitionDefinition.getBeAddress(),
                partitionDefinition.getTabletIds());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisSourceSplit that = (DorisSourceSplit) o;

        return Objects.equals(partitionDefinition, that.partitionDefinition);
    }
}
