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

import org.apache.doris.flink.sink.OptionUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Unit tests for the {@link DorisSourceSplitSerializer}. */
public class DorisSourceSplitSerializerTest {

    @Test
    public void serializeSplit() throws Exception {
        final DorisSourceSplit split =
                new DorisSourceSplit("splitId", OptionUtils.buildPartitionDef());

        DorisSourceSplit deSerialized = serializeAndDeserializeSplit(split);
        assertEquals(split, deSerialized);
    }

    private DorisSourceSplit serializeAndDeserializeSplit(DorisSourceSplit split) throws Exception {
        final DorisSourceSplitSerializer splitSerializer = new DorisSourceSplitSerializer();
        byte[] serialized = splitSerializer.serialize(split);
        return splitSerializer.deserialize(splitSerializer.getVersion(), serialized);
    }
}
