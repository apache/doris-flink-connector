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

import org.junit.Assert;
import org.junit.Test;

/** test for DorisWriterStateSerializer. */
public class TestDorisWriterStateSerializer {
    @Test
    public void testSerialize() throws Exception {
        DorisWriterState expectDorisWriterState = new DorisWriterState("doris", "db", "table", 0);
        DorisWriterStateSerializer serializer = new DorisWriterStateSerializer();
        DorisWriterState dorisWriterState =
                serializer.deserialize(2, serializer.serialize(expectDorisWriterState));
        Assert.assertEquals(expectDorisWriterState, dorisWriterState);

        DorisWriterState expectDorisWriterStateV1 = new DorisWriterState("doris");
        serializer = new DorisWriterStateSerializer();
        DorisWriterState dorisWriterStatev1 =
                serializer.deserialize(1, serializer.serialize(expectDorisWriterState));
        Assert.assertEquals(expectDorisWriterStateV1, dorisWriterStatev1);

        DorisWriterState state = new DorisWriterState("doris", "db", "table", 0);
        DorisWriterState state1 = new DorisWriterState("doris", "db", "table", 0);
        DorisWriterState state2 = new DorisWriterState("doris", "db1", "table", 0);
        DorisWriterState state3 = new DorisWriterState("doris", "db", "table1", 0);
        DorisWriterState state4 = new DorisWriterState("doris", "db", "table", 1);
        Assert.assertEquals(state, state1);
        Assert.assertEquals(state, state);
        Assert.assertNotEquals(state, state2);
        Assert.assertNotEquals(state, state3);
        Assert.assertNotEquals(state, state4);
    }
}
