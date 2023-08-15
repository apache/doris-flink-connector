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

package org.apache.doris.flink.sink.batch;

import org.apache.flink.annotation.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * buffer to queue
 */
public class BatchRecordBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(BatchRecordBuffer.class);
    private static final String LINE_SEPARATOR = "\n";
    private String labelName;
    private ByteBuffer buffer;
    private byte[] lineDelimiter;
    private int numOfRecords = 0;
    private int bufferSizeBytes = 0;
    private boolean loadBatchFirstRecord = true;

    public BatchRecordBuffer() {
    }

    public BatchRecordBuffer(byte[] lineDelimiter, int bufferSize) {
        super();
        this.lineDelimiter = lineDelimiter;
        this.buffer = ByteBuffer.allocate(bufferSize);
    }

    public void insert(byte[] record) {
        ensureCapacity(record.length);
        if (loadBatchFirstRecord) {
            loadBatchFirstRecord = false;
        } else {
            this.buffer.put(this.lineDelimiter);
        }
        this.buffer.put(record);
        setNumOfRecords(getNumOfRecords() + 1);
        setBufferSizeBytes(getBufferSizeBytes() + record.length);
    }

    @VisibleForTesting
    public void ensureCapacity(int length) {
        if (buffer.remaining() >= length) {
            return;
        }
        int currentRemain = buffer.remaining();
        int currentCapacity = buffer.capacity();

        int target = buffer.remaining() + length;
        int capacity = buffer.capacity();
        // grow 512kb each time
        target = Math.max(target, Math.min(capacity + 512 * 1024, capacity * 2));
        ByteBuffer tmp = ByteBuffer.allocate(target);
        buffer.flip();
        tmp.put(buffer);
        buffer.clear();
        buffer = tmp;
        LOG.info("record length {},buffer remain {} ,grow capacity {} to {}", length, currentRemain, currentCapacity,
                target);
    }

    public String getLabelName() {
        return labelName;
    }

    public void setLabelName(String labelName) {
        this.labelName = labelName;
    }

    /**
     * @return true if buffer is empty
     */
    public boolean isEmpty() {
        return numOfRecords == 0;
    }

    public ByteBuffer getData() {
        // change mode
        buffer.flip();
        LOG.debug("flush buffer: {} records, {} bytes", getNumOfRecords(), getBufferSizeBytes());
        return buffer;
    }

    public void clear() {
        this.buffer.clear();
        this.numOfRecords = 0;
        this.bufferSizeBytes = 0;
        this.labelName = null;
        this.loadBatchFirstRecord = true;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    /**
     * @return Number of records in this buffer
     */
    public int getNumOfRecords() {
        return numOfRecords;
    }

    /**
     * @return Buffer size in bytes
     */
    public int getBufferSizeBytes() {
        return bufferSizeBytes;
    }

    /**
     * @param numOfRecords Updates number of records (Usually by 1)
     */
    public void setNumOfRecords(int numOfRecords) {
        this.numOfRecords = numOfRecords;
    }

    /**
     * @param bufferSizeBytes Updates sum of size of records present in this buffer (Bytes)
     */
    public void setBufferSizeBytes(int bufferSizeBytes) {
        this.bufferSizeBytes = bufferSizeBytes;
    }

}
