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

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/** Unit tests for {@link DorisSourceSplitReader}, focusing on reader lifecycle. */
public class DorisSourceSplitReaderTest {

    /** A ValueReader that records how many times close() is invoked. hasNext() returns true so
     * fetch() does not finish the split, leaving the reader open for close() to exercise. */
    private static final class RecordingValueReader extends ValueReader {
        final AtomicInteger closeCount = new AtomicInteger(0);

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public List next() {
            return Collections.emptyList();
        }

        @Override
        public void close() throws Exception {
            closeCount.incrementAndGet();
        }
    }

    /** A ValueReader whose close() always throws, to verify best-effort swallowing + field nulling. */
    private static final class ThrowingValueReader extends ValueReader {
        final AtomicInteger closeCount = new AtomicInteger(0);

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public List next() {
            return Collections.emptyList();
        }

        @Override
        public void close() throws Exception {
            closeCount.incrementAndGet();
            throw new RuntimeException("simulated thrift close failure");
        }
    }

    /** A subclass that injects the recording reader instead of opening a real BE connection. */
    private static final class TestSplitReader extends DorisSourceSplitReader {
        final RecordingValueReader injectedReader = new RecordingValueReader();

        TestSplitReader() {
            super(
                    new DorisOptions.Builder()
                            .setFenodes("127.0.0.1:8030")
                            .setTableIdentifier("db.table")
                            .build(),
                    DorisReadOptions.defaults());
            handleSplitsChanges(
                    new SplitsAddition<>(
                            Collections.singletonList(
                                    new DorisSourceSplit(
                                            "test-split",
                                            PartitionDefinition.emptyPartition("table")))));
        }

        @Override
        protected ValueReader createValueReader(DorisSourceSplit split) {
            return injectedReader;
        }
    }

    /** A subclass that injects the throwing reader. */
    private static final class ThrowingSplitReader extends DorisSourceSplitReader {
        final ThrowingValueReader injectedReader = new ThrowingValueReader();

        ThrowingSplitReader() {
            super(
                    new DorisOptions.Builder()
                            .setFenodes("127.0.0.1:8030")
                            .setTableIdentifier("db.table")
                            .build(),
                    DorisReadOptions.defaults());
            handleSplitsChanges(
                    new SplitsAddition<>(
                            Collections.singletonList(
                                    new DorisSourceSplit(
                                            "test-split",
                                            PartitionDefinition.emptyPartition("table")))));
        }

        @Override
        protected ValueReader createValueReader(DorisSourceSplit split) {
            return injectedReader;
        }
    }

    @Test
    public void closeWithoutReaderIsNoop() throws Exception {
        // valueReader is null (no fetch yet). close() must not throw.
        TestSplitReader reader = new TestSplitReader();
        reader.close();

        assertEquals(
                "close() without a reader must not invoke any reader", 0,
                reader.injectedReader.closeCount.get());
    }

    @Test
    public void closeReleasesValueReader() throws Exception {
        TestSplitReader reader = new TestSplitReader();
        // fetch() injects the fake reader (hasNext==true so the split is not finished) leaving it open.
        reader.fetch();

        reader.close();

        assertEquals(
                "close() must release the value reader exactly once", 1,
                reader.injectedReader.closeCount.get());
    }

    @Test
    public void closeSwallowsExceptionAndNullsField() throws Exception {
        ThrowingSplitReader reader = new ThrowingSplitReader();
        reader.fetch();

        // close() must NOT propagate the reader's exception (best-effort teardown).
        reader.close();
        assertEquals(
                "close() must invoke the reader's close() once even when it throws", 1,
                reader.injectedReader.closeCount.get());
        // Field was nulled in finally: a second close() must be a no-op (no second invocation).
        reader.close();
        assertEquals(
                "close() must null the reader in finally so a second close() is a no-op", 1,
                reader.injectedReader.closeCount.get());
    }

    @Test
    public void closeIsIdempotent() throws Exception {
        TestSplitReader reader = new TestSplitReader();
        reader.fetch();

        reader.close();
        reader.close();
        reader.close();

        assertEquals(
                "repeated close() must invoke the reader's close() exactly once", 1,
                reader.injectedReader.closeCount.get());
    }
}
