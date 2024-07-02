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

import org.apache.doris.flink.sink.OptionUtils;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.source.split.DorisSourceSplitSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/** Unit tests for the {@link PendingSplitsCheckpointSerializer}. */
public class PendingSplitsCheckpointSerializerTest {

    private static void assertCheckpointsEqual(
            final PendingSplitsCheckpoint expected, final PendingSplitsCheckpoint actual) {
        Assert.assertEquals(expected.getSplits(), actual.getSplits());
    }

    @Test
    public void serializeSplit() throws Exception {
        final DorisSourceSplit split =
                new DorisSourceSplit("splitId", OptionUtils.buildPartitionDef());
        PendingSplitsCheckpoint checkpoint = new PendingSplitsCheckpoint(Arrays.asList(split));

        final PendingSplitsCheckpointSerializer splitSerializer =
                new PendingSplitsCheckpointSerializer(DorisSourceSplitSerializer.INSTANCE);
        byte[] serialized = splitSerializer.serialize(checkpoint);
        PendingSplitsCheckpoint deserialize =
                splitSerializer.deserialize(splitSerializer.getVersion(), serialized);

        assertCheckpointsEqual(checkpoint, deserialize);
    }
}
