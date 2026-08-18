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

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.doris.flink.sink.writer.DorisWriterState;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/** Shared writer that stages JSON Lines files in S3-compatible object storage. */
public class S3TvfWriter<IN> {

    private static final byte NEW_LINE = '\n';
    private static final int UPLOAD_QUEUE_SIZE = 1;

    private final int subtaskId;
    private final DorisRecordSerializer<IN> serializer;
    private final S3ObjectStore objectStore;
    private final String database;
    private final String table;
    private final String objectPrefix;
    private final String labelPrefix;
    private final List<String> columns;
    private final boolean deleteSignEnabled;
    private final int maxBytes;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private final List<String> currentObjectKeys = new ArrayList<>();
    private final BlockingQueue<Runnable> uploadQueue =
            new LinkedBlockingQueue<>(UPLOAD_QUEUE_SIZE);
    private final AtomicReference<IOException> uploadException = new AtomicReference<>();
    private final ExecutorService uploadExecutor;

    private long currentCheckpointId;
    private int fileNumber;

    public S3TvfWriter(
            long restoredCheckpointId,
            int subtaskId,
            DorisRecordSerializer<IN> serializer,
            S3ObjectStore objectStore,
            String database,
            String table,
            String objectPrefix,
            String labelPrefix,
            List<String> columns,
            boolean deleteSignEnabled,
            int maxBytes) {
        Preconditions.checkArgument(maxBytes > 0, "TVF buffer max bytes must be positive.");
        this.currentCheckpointId = restoredCheckpointId + 1;
        this.subtaskId = subtaskId;
        this.serializer = serializer;
        this.objectStore = objectStore;
        this.database = database;
        this.table = table;
        this.objectPrefix = objectPrefix;
        this.labelPrefix = labelPrefix;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.deleteSignEnabled = deleteSignEnabled;
        this.maxBytes = maxBytes;
        this.uploadExecutor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("s3-tvf-upload-" + subtaskId));
        this.uploadExecutor.execute(this::processUploads);
        serializer.initial();
    }

    public void write(IN value) throws IOException {
        checkUploadException();
        append(serializer.serialize(value));
    }

    public void flush() throws IOException {
        append(serializer.flush());
        uploadBuffer();
        waitForUploads();
    }

    public Collection<S3TvfCommittable> prepareCommit() throws IOException {
        uploadBuffer();
        waitForUploads();
        if (currentObjectKeys.isEmpty()) {
            return Collections.emptyList();
        }
        String label =
                String.format("%s_%s_%d_%d", labelPrefix, table, subtaskId, currentCheckpointId);
        return Collections.singletonList(
                new S3TvfCommittable(
                        currentCheckpointId,
                        database,
                        table,
                        label,
                        currentObjectKeys,
                        columns,
                        deleteSignEnabled));
    }

    public List<DorisWriterState> snapshotState(long checkpointId) {
        currentObjectKeys.clear();
        currentCheckpointId = checkpointId + 1;
        fileNumber = 0;
        return Collections.emptyList();
    }

    public void close() throws Exception {
        try {
            serializer.close();
        } finally {
            uploadExecutor.shutdownNow();
            objectStore.close();
        }
    }

    private void append(DorisRecord record) throws IOException {
        if (record == null || record.getRow() == null || record.getRow().length == 0) {
            return;
        }
        if (record.getTableIdentifier() != null
                && !record.getTableIdentifier().equals(database + "." + table)) {
            throw new IOException("TVF write mode does not support dynamic table routing.");
        }

        byte[] row = record.getRow();
        int bytesWithNewLine = row.length + 1;
        if (buffer.size() > 0 && buffer.size() + bytesWithNewLine > maxBytes) {
            uploadBuffer();
        }
        buffer.write(row);
        buffer.write(NEW_LINE);
        if (buffer.size() >= maxBytes) {
            uploadBuffer();
        }
    }

    private void uploadBuffer() throws IOException {
        if (buffer.size() == 0) {
            return;
        }
        String fileName =
                String.format(
                        "%s_%s_%d_%d_%d.json",
                        labelPrefix, table, subtaskId, currentCheckpointId, fileNumber++);
        String objectKey = objectPrefix + (objectPrefix.endsWith("/") ? "" : "/") + fileName;
        byte[] content = buffer.toByteArray();
        putUpload(
                () -> {
                    if (uploadException.get() != null) {
                        return;
                    }
                    try {
                        objectStore.put(objectKey, content);
                        currentObjectKeys.add(objectKey);
                    } catch (Exception e) {
                        IOException failure =
                                e instanceof IOException
                                        ? (IOException) e
                                        : new IOException(
                                                "Failed to upload object '" + objectKey + "'.", e);
                        uploadException.compareAndSet(null, failure);
                    }
                });
        buffer.reset();
    }

    private void processUploads() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                uploadQueue.take().run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void waitForUploads() throws IOException {
        for (int i = 0; i <= UPLOAD_QUEUE_SIZE; i++) {
            putUpload(() -> {});
        }
        checkUploadException();
    }

    private void putUpload(Runnable upload) throws IOException {
        checkUploadException();
        try {
            uploadQueue.put(upload);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while adding an S3 upload to the queue.", e);
        }
        checkUploadException();
    }

    private void checkUploadException() throws IOException {
        IOException exception = uploadException.get();
        if (exception != null) {
            throw exception;
        }
    }
}
