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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Serializer for {@link S3TvfCommittable}. */
public class S3TvfCommittableSerializer implements SimpleVersionedSerializer<S3TvfCommittable> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(S3TvfCommittable committable) throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeLong(committable.getCheckpointId());
            out.writeUTF(committable.getDatabase());
            out.writeUTF(committable.getTable());
            out.writeUTF(committable.getLabel());
            writeStrings(out, committable.getObjectKeys());
            writeStrings(out, committable.getColumns());
            out.writeBoolean(committable.isDeleteSignEnabled());
            out.flush();
            return bytes.toByteArray();
        }
    }

    @Override
    public S3TvfCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported S3 TVF committable version: " + version);
        }
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bytes)) {
            long checkpointId = in.readLong();
            String database = in.readUTF();
            String table = in.readUTF();
            String label = in.readUTF();
            List<String> objectKeys = readStrings(in);
            List<String> columns = readStrings(in);
            boolean deleteSignEnabled = in.readBoolean();
            return new S3TvfCommittable(
                    checkpointId, database, table, label, objectKeys, columns, deleteSignEnabled);
        }
    }

    private static void writeStrings(DataOutputStream out, List<String> values) throws IOException {
        out.writeInt(values.size());
        for (String value : values) {
            out.writeUTF(value);
        }
    }

    private static List<String> readStrings(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0) {
            throw new IOException("Negative string list size: " + size);
        }
        List<String> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(in.readUTF());
        }
        return values;
    }
}
