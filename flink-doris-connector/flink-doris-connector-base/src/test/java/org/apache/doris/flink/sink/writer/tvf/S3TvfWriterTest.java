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

import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class S3TvfWriterTest {

    @Test
    public void testFlushByBytesAndBuildDeterministicCommittable() throws Exception {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        S3TvfWriter<String> writer = createWriter(6L, objectStore);

        writer.write("12345");
        writer.write("67890");
        writer.flush();

        Assert.assertEquals(
                Arrays.asList("prefix/label_tbl_2_7_0.json", "prefix/label_tbl_2_7_1.json"),
                objectStore.objectKeys);
        Assert.assertArrayEquals(
                "12345\n".getBytes(StandardCharsets.UTF_8), objectStore.contents.get(0));
        Assert.assertArrayEquals(
                "67890\n".getBytes(StandardCharsets.UTF_8), objectStore.contents.get(1));

        Collection<S3TvfCommittable> committables = writer.prepareCommit();
        Assert.assertEquals(1, committables.size());
        S3TvfCommittable committable = committables.iterator().next();
        Assert.assertEquals(7L, committable.getCheckpointId());
        Assert.assertEquals("label_tbl_2_7", committable.getLabel());
        Assert.assertEquals(objectStore.objectKeys, committable.getObjectKeys());
        Assert.assertEquals(Arrays.asList("id", "name"), committable.getColumns());
        Assert.assertTrue(committable.isDeleteSignEnabled());
    }

    @Test
    public void testAdvanceCheckpointAndResetFileNumber() throws Exception {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        S3TvfWriter<String> writer = createWriter(6L, objectStore);
        writer.write("first");
        writer.flush();
        writer.prepareCommit();

        writer.snapshotState(7L);
        writer.write("next");
        writer.flush();

        Assert.assertEquals("prefix/label_tbl_2_8_0.json", objectStore.objectKeys.get(1));
        Assert.assertEquals("label_tbl_2_8", writer.prepareCommit().iterator().next().getLabel());
    }

    @Test
    public void testAdvanceAcrossSkippedCheckpointId() throws Exception {
        RecordingObjectStore objectStore = new RecordingObjectStore();
        S3TvfWriter<String> writer = createWriter(9L, objectStore);
        writer.write("replayed");
        writer.flush();

        S3TvfCommittable replayed = writer.prepareCommit().iterator().next();
        Assert.assertEquals(10L, replayed.getCheckpointId());
        Assert.assertEquals("label_tbl_2_10", replayed.getLabel());

        writer.snapshotState(11L);
        writer.write("next");
        writer.flush();

        S3TvfCommittable next = writer.prepareCommit().iterator().next();
        Assert.assertEquals(12L, next.getCheckpointId());
        Assert.assertEquals("label_tbl_2_12", next.getLabel());
        Assert.assertEquals("prefix/label_tbl_2_12_0.json", objectStore.objectKeys.get(1));
    }

    private static S3TvfWriter<String> createWriter(
            long restoredCheckpointId, RecordingObjectStore objectStore) {
        DorisRecordSerializer<String> serializer =
                value -> DorisRecord.of(value.getBytes(StandardCharsets.UTF_8));
        return new S3TvfWriter<>(
                restoredCheckpointId,
                2,
                serializer,
                objectStore,
                "db",
                "tbl",
                "prefix",
                "label",
                Arrays.asList("id", "name"),
                true,
                10);
    }

    private static class RecordingObjectStore implements S3ObjectStore {
        private final List<String> objectKeys = new ArrayList<>();
        private final List<byte[]> contents = new ArrayList<>();

        @Override
        public void put(String objectKey, byte[] content) {
            objectKeys.add(objectKey);
            contents.add(content);
        }

        @Override
        public void close() throws IOException {}
    }
}
