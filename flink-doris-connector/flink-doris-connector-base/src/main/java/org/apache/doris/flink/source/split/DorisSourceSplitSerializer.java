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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.doris.flink.rest.PartitionDefinition;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;

/** Versioned serializer for snapshot and stream Doris source splits. */
public class DorisSourceSplitSerializer implements SimpleVersionedSerializer<DorisSourceSplit> {
    public static final DorisSourceSplitSerializer INSTANCE = new DorisSourceSplitSerializer();

    private static final int VERSION = 3;
    private static final byte SNAPSHOT_KIND = 0;
    private static final byte STREAM_KIND = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DorisSourceSplit split) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            if (split instanceof DorisSnapshotSplit) {
                out.writeByte(SNAPSHOT_KIND);
                writeSnapshot(out, (DorisSnapshotSplit) split);
            } else if (split instanceof DorisStreamSplit) {
                out.writeByte(STREAM_KIND);
                writeStream(out, (DorisStreamSplit) split);
            } else {
                throw new IOException("Unknown Doris source split type: " + split.getClass());
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public DorisSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            if (version == 1 || version == 2) {
                return readLegacySnapshot(version, in);
            }
            if (version != VERSION) {
                throw new IOException("Unknown version: " + version);
            }
            byte kind = in.readByte();
            if (kind == SNAPSHOT_KIND) {
                return readSnapshot(in);
            }
            if (kind == STREAM_KIND) {
                return readStream(in);
            }
            throw new IOException("Unknown Doris source split kind: " + kind);
        }
    }

    private static DorisSnapshotSplit readLegacySnapshot(int version, DataInputStream in)
            throws IOException {
        String database = in.readUTF();
        String table = in.readUTF();
        String beAddress = in.readUTF();
        int tabletCount = in.readInt();
        if (tabletCount < 0) {
            throw new IOException("Negative tablet count: " + tabletCount);
        }
        Set<Long> tabletIds = new LinkedHashSet<>(tabletCount);
        for (int index = 0; index < tabletCount; index++) {
            tabletIds.add(in.readLong());
        }
        String queryPlan = readQueryPlan(in);
        String splitId = version >= 2 ? in.readUTF() : "splitId";
        return new DorisSnapshotSplit(
                splitId, new PartitionDefinition(database, table, beAddress, tabletIds, queryPlan));
    }

    private static void writeSnapshot(DataOutputStream out, DorisSnapshotSplit split)
            throws IOException {
        PartitionDefinition partition = split.getPartitionDefinition();
        out.writeUTF(split.splitId());
        out.writeUTF(partition.getDatabase());
        out.writeUTF(partition.getTable());
        out.writeUTF(partition.getBeAddress());
        out.writeInt(partition.getTabletIds().size());
        for (Long tabletId : new java.util.TreeSet<>(partition.getTabletIds())) {
            out.writeLong(tabletId);
        }
        byte[] queryPlan = partition.getQueryPlan().getBytes(StandardCharsets.UTF_8);
        out.writeInt(queryPlan.length);
        out.write(queryPlan);
    }

    private static DorisSnapshotSplit readSnapshot(DataInputStream in) throws IOException {
        String splitId = in.readUTF();
        String database = in.readUTF();
        String table = in.readUTF();
        String beAddress = in.readUTF();
        int tabletCount = in.readInt();
        if (tabletCount < 0) {
            throw new IOException("Negative tablet count: " + tabletCount);
        }
        Set<Long> tabletIds = new LinkedHashSet<>(tabletCount);
        for (int index = 0; index < tabletCount; index++) {
            tabletIds.add(in.readLong());
        }
        String queryPlan = readQueryPlan(in);
        return new DorisSnapshotSplit(
                splitId, new PartitionDefinition(database, table, beAddress, tabletIds, queryPlan));
    }

    private static String readQueryPlan(DataInputStream in) throws IOException {
        int queryPlanLength = in.readInt();
        if (queryPlanLength < 0) {
            throw new IOException("Negative query plan length: " + queryPlanLength);
        }
        byte[] queryPlanBytes = new byte[queryPlanLength];
        in.readFully(queryPlanBytes);
        return new String(queryPlanBytes, StandardCharsets.UTF_8);
    }

    private static void writeStream(DataOutputStream out, DorisStreamSplit split)
            throws IOException {
        out.writeUTF(split.splitId());
        out.writeUTF(split.getStartTimestamp());
        out.writeUTF(split.getEndTimestamp());
    }

    private static DorisStreamSplit readStream(DataInputStream in) throws IOException {
        try {
            return new DorisStreamSplit(in.readUTF(), in.readUTF(), in.readUTF());
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid stream split", e);
        }
    }
}
