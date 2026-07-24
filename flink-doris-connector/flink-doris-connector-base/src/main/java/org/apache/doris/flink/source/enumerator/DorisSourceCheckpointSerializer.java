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

package org.apache.doris.flink.source.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.doris.flink.source.split.DorisSourceSplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Serializer for complete Doris source enumerator checkpoints. */
public final class DorisSourceCheckpointSerializer
        implements SimpleVersionedSerializer<DorisSourceCheckpoint> {
    private static final int VERSION = 3;

    private final SimpleVersionedSerializer<DorisSourceSplit> splitSerializer;

    public DorisSourceCheckpointSerializer(
            SimpleVersionedSerializer<DorisSourceSplit> splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DorisSourceCheckpoint checkpoint) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeByte(checkpoint.getPhase().ordinal());
            writeNullableString(out, checkpoint.getNextStreamStartTimestamp());
            out.writeInt(checkpoint.getSourceParallelism());
            out.writeInt(splitSerializer.getVersion());
            out.writeInt(checkpoint.getPendingSplits().size());
            for (DorisSourceSplit split : checkpoint.getPendingSplits()) {
                writeSplit(out, split);
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public DorisSourceCheckpoint deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            int phaseOrdinal = in.readByte();
            DorisSourceCheckpoint.Phase[] phases = DorisSourceCheckpoint.Phase.values();
            if (phaseOrdinal < 0 || phaseOrdinal >= phases.length) {
                throw new IOException("Unknown checkpoint phase: " + phaseOrdinal);
            }
            String nextStreamStartTimestamp = readNullableString(in);
            int sourceParallelism = in.readInt();
            int splitVersion = in.readInt();
            int pendingCount = readCount(in, "pending split");
            List<DorisSourceSplit> pendingSplits = new ArrayList<>(pendingCount);
            for (int index = 0; index < pendingCount; index++) {
                pendingSplits.add(readSplit(in, splitVersion));
            }
            return new DorisSourceCheckpoint(
                    phases[phaseOrdinal],
                    nextStreamStartTimestamp,
                    sourceParallelism,
                    pendingSplits);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid Doris source checkpoint", e);
        }
    }

    private void writeSplit(DataOutputStream out, DorisSourceSplit split) throws IOException {
        byte[] serialized = splitSerializer.serialize(split);
        out.writeInt(serialized.length);
        out.write(serialized);
    }

    private DorisSourceSplit readSplit(DataInputStream in, int splitVersion) throws IOException {
        int length = readCount(in, "split payload");
        byte[] serialized = new byte[length];
        in.readFully(serialized);
        return splitSerializer.deserialize(splitVersion, serialized);
    }

    private static void writeNullableString(DataOutputStream out, String value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            out.writeUTF(value);
        }
    }

    private static String readNullableString(DataInputStream in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }

    private static int readCount(DataInputStream in, String name) throws IOException {
        int count = in.readInt();
        if (count < 0) {
            throw new IOException("Negative " + name + " count: " + count);
        }
        return count;
    }
}
