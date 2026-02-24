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

package org.apache.doris.flink.source.reader;

import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/** Unit tests for the {@link DorisSourceReader}. */
public class DorisSourceReaderTest {

    private static DorisSourceReader createReader(TestingReaderContext context) {
        return new DorisSourceReader<>(
                OptionUtils.buildDorisOptions(),
                OptionUtils.buildDorisReadOptions(),
                new DorisRecordEmitter<>(new SimpleListDeserializationSchema()),
                context,
                context.getConfiguration());
    }

    private static DorisSourceSplit createTestDorisSplit() throws IOException {
        return new DorisSourceSplit("splitId", OptionUtils.buildPartitionDef());
    }

    @Test
    public void testRequestSplitWhenNoSplitRestored() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        final DorisSourceReader reader = createReader(context);

        reader.start();
        reader.close();
        assertEquals(1, context.getNumSplitRequests());
    }

    @Test
    public void testNoSplitRequestWhenSplitRestored() throws Exception {
        final TestingReaderContext context = new TestingReaderContext();
        final DorisSourceReader reader = createReader(context);

        reader.addSplits(Collections.singletonList(createTestDorisSplit()));
        reader.start();
        reader.close();

        assertEquals(0, context.getNumSplitRequests());
    }
}
