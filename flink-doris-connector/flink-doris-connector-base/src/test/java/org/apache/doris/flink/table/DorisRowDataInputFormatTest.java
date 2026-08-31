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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.doris.flink.table;

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.source.reader.ValueReader;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/** Unit tests for {@link DorisRowDataInputFormat}. */
public class DorisRowDataInputFormatTest {

    @Test
    public void testBuilderPropagatesTlsOptions() {
        DorisRowDataInputFormat inputFormat =
                DorisRowDataInputFormat.builder()
                        .setFenodes("fe:8030")
                        .setTableIdentifier("db.table")
                        .setTlsEnabled(true)
                        .setTlsCaCertificatePath("/etc/doris/ca.pem")
                        .setTlsSkipHostnameVerification(true)
                        .setTlsExcludedProtocols("arrowflight")
                        .setPartitions(Collections.emptyList())
                        .setReadOptions(DorisReadOptions.defaults())
                        .setRowType(
                                RowType.of(
                                        new LogicalType[] {new VarCharType()}, new String[] {"c1"}))
                        .build();

        DorisTlsOptions tlsOptions = inputFormat.getOptions().getTlsOptions();
        Assert.assertTrue(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.HTTP));
        Assert.assertTrue(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.MYSQL));
        Assert.assertFalse(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.ARROW_FLIGHT));
        Assert.assertEquals("/etc/doris/ca.pem", tlsOptions.getCaCertificatePath());
        Assert.assertTrue(tlsOptions.isSkipHostnameVerification());
    }

    // --- Reader lifecycle (close() resource-leak) tests ---

    /** A ValueReader that records how many times close() is invoked. */
    private static final class RecordingValueReader extends ValueReader {
        final AtomicInteger closeCount = new AtomicInteger(0);

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public List<Object> next() {
            return Collections.emptyList();
        }

        @Override
        public void close() throws Exception {
            closeCount.incrementAndGet();
        }
    }

    /** A subclass that injects the recording reader instead of opening a real BE connection. */
    private static final class TestInputFormat extends DorisRowDataInputFormat {
        final RecordingValueReader injectedReader = new RecordingValueReader();

        TestInputFormat() {
            // partitions are unused for close() testing; rowType is a single INT column.
            super(
                    new org.apache.doris.flink.cfg.DorisOptions.Builder()
                            .setFenodes("127.0.0.1:8030")
                            .setTableIdentifier("db.table")
                            .build(),
                    Collections.emptyList(),
                    DorisReadOptions.defaults(),
                    RowType.of(new IntType()));
        }

        @Override
        protected ValueReader createValueReader(PartitionDefinition partition) {
            return injectedReader;
        }
    }

    @Test
    public void closeReleasesValueReader() throws Exception {
        TestInputFormat format = new TestInputFormat();
        DorisTableInputSplit split =
                new DorisTableInputSplit(0, PartitionDefinition.emptyPartition("table"));

        format.open(split);
        format.close();

        assertEquals(
                "close() must release the value reader exactly once",
                1,
                format.injectedReader.closeCount.get());
    }

    @Test
    public void closeWithoutOpenIsNoop() throws Exception {
        TestInputFormat format = new TestInputFormat();
        // Never call open(); valueReader is null. close() must not throw.
        format.close();

        assertEquals(
                "close() without open() must not invoke any reader",
                0,
                format.injectedReader.closeCount.get());
    }

    /**
     * A ValueReader whose close() always throws, to verify best-effort swallowing + field nulling.
     */
    private static final class ThrowingValueReader extends ValueReader {
        final AtomicInteger closeCount = new AtomicInteger(0);

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public List<Object> next() {
            return Collections.emptyList();
        }

        @Override
        public void close() throws Exception {
            closeCount.incrementAndGet();
            throw new RuntimeException("simulated thrift close failure");
        }
    }

    /** A subclass that injects the throwing reader. */
    private static final class ThrowingInputFormat extends DorisRowDataInputFormat {
        final ThrowingValueReader injectedReader = new ThrowingValueReader();

        ThrowingInputFormat() {
            super(
                    new org.apache.doris.flink.cfg.DorisOptions.Builder()
                            .setFenodes("127.0.0.1:8030")
                            .setTableIdentifier("db.table")
                            .build(),
                    Collections.emptyList(),
                    DorisReadOptions.defaults(),
                    RowType.of(new IntType()));
        }

        @Override
        protected ValueReader createValueReader(PartitionDefinition partition) {
            return injectedReader;
        }
    }

    @Test
    public void closeSwallowsExceptionAndNullsField() throws Exception {
        ThrowingInputFormat format = new ThrowingInputFormat();
        format.open(new DorisTableInputSplit(0, PartitionDefinition.emptyPartition("table")));

        // close() must NOT propagate the reader's exception (best-effort teardown).
        format.close();
        // The reader's close() was invoked exactly once.
        assertEquals(
                "close() must invoke the reader's close() once even when it throws",
                1,
                format.injectedReader.closeCount.get());
        // Field was nulled in finally: a second close() must be a no-op (no second invocation).
        format.close();
        assertEquals(
                "close() must null the reader in finally so a second close() is a no-op",
                1,
                format.injectedReader.closeCount.get());
    }

    @Test
    public void closeIsIdempotent() throws Exception {
        TestInputFormat format = new TestInputFormat();
        format.open(new DorisTableInputSplit(0, PartitionDefinition.emptyPartition("table")));

        format.close();
        format.close();
        format.close();

        assertEquals(
                "repeated close() must invoke the reader's close() exactly once",
                1,
                format.injectedReader.closeCount.get());
    }
}
