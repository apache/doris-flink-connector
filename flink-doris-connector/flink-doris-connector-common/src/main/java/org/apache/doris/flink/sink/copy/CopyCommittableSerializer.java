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

package org.apache.doris.flink.sink.copy;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** define how to serialize DorisCopyCommittable. */
public class CopyCommittableSerializer implements SimpleVersionedSerializer<DorisCopyCommittable> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DorisCopyCommittable copyCommittable) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(copyCommittable.getHostPort());

            // writeUTF has a length limit, but the copysql is sometimes very long
            final byte[] copySqlBytes =
                    copyCommittable.getCopySQL().getBytes(StandardCharsets.UTF_8);
            out.writeInt(copySqlBytes.length);
            out.write(copySqlBytes);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public DorisCopyCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            final String hostPort = in.readUTF();

            // read copySQL
            final int len = in.readInt();
            final byte[] bytes = new byte[len];
            in.read(bytes);
            String copySQL = new String(bytes, StandardCharsets.UTF_8);
            return new DorisCopyCommittable(hostPort, copySQL);
        }
    }
}
