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

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializer for DorisWriterState. */
public class DorisWriterStateSerializer implements SimpleVersionedSerializer<DorisWriterState> {

    private static final int VERSION = 2;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DorisWriterState dorisWriterState) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(dorisWriterState.getLabelPrefix());
            out.writeUTF(dorisWriterState.getDatabase());
            out.writeUTF(dorisWriterState.getTable());
            out.writeInt(dorisWriterState.getSubtaskId());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public DorisWriterState deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            String labelPrefix = in.readUTF();
            if (version == 1) {
                return new DorisWriterState(labelPrefix);
            } else {
                final String database = in.readUTF();
                final String table = in.readUTF();
                final int subtaskId = in.readInt();
                return new DorisWriterState(labelPrefix, database, table, subtaskId);
            }
        }
    }
}
