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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.doris.flink.rest.PartitionDefinition;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** A serializer for the {@link DorisSourceSplit}. */
public class DorisSourceSplitSerializer implements SimpleVersionedSerializer<DorisSourceSplit> {

    public static final DorisSourceSplitSerializer INSTANCE = new DorisSourceSplitSerializer();

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int VERSION = 2;

    private static void writeLongArray(DataOutputView out, Long[] values) throws IOException {
        out.writeInt(values.length);
        for (Long val : values) {
            out.writeLong(val);
        }
    }

    private static Long[] readLongArray(DataInputView in) throws IOException {
        final int len = in.readInt();
        final Long[] values = new Long[len];
        for (int i = 0; i < len; i++) {
            values[i] = in.readLong();
        }
        return values;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DorisSourceSplit split) throws IOException {

        // optimization: the splits lazily cache their own serialized form
        if (split.serializedFormCache != null) {
            return split.serializedFormCache;
        }

        final DataOutputSerializer out = SERIALIZER_CACHE.get();

        PartitionDefinition partDef = split.getPartitionDefinition();
        out.writeUTF(partDef.getDatabase());
        out.writeUTF(partDef.getTable());
        out.writeUTF(partDef.getBeAddress());
        writeLongArray(out, partDef.getTabletIds().toArray(new Long[] {}));
        // writeUTF has a length limit, but the query plan is sometimes very long
        final byte[] queryPlanBytes = partDef.getQueryPlan().getBytes(StandardCharsets.UTF_8);
        out.writeInt(queryPlanBytes.length);
        out.write(queryPlanBytes);

        out.writeUTF(split.splitId());

        final byte[] result = out.getCopyOfBuffer();
        out.clear();

        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        split.serializedFormCache = result;

        return result;
    }

    @Override
    public DorisSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
            case 2:
                return deserializeSplit(version, serialized);
            default:
                throw new IOException("Unknown version: " + version);
        }
    }

    private DorisSourceSplit deserializeSplit(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final String database = in.readUTF();
        final String table = in.readUTF();
        final String beAddress = in.readUTF();
        Long[] vals = readLongArray(in);
        final Set<Long> tabletIds = new HashSet<>(Arrays.asList(vals));

        // read query plan
        final int len = in.readInt();
        final byte[] bytes = new byte[len];
        in.read(bytes);
        final String queryPlan = new String(bytes, StandardCharsets.UTF_8);

        // read split id
        String splitId = "splitId";
        if (version >= 2) {
            splitId = in.readUTF();
        }
        PartitionDefinition partDef =
                new PartitionDefinition(database, table, beAddress, tabletIds, queryPlan);
        return new DorisSourceSplit(splitId, partDef);
    }
}
