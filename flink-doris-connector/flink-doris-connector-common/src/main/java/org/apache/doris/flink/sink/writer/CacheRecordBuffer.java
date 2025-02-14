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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

/** Channel of record stream and HTTP data stream. */
public class CacheRecordBuffer extends RecordBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(CacheRecordBuffer.class);
    BlockingDeque<ByteBuffer> bufferCache;
    LinkedBlockingQueue<ByteBuffer> bufferPool;

    public CacheRecordBuffer(int capacity, int queueSize) {
        super(capacity, queueSize);
        bufferCache = new LinkedBlockingDeque<>();
        bufferPool = new LinkedBlockingQueue<>();
    }

    @Override
    public void startBufferData() throws IOException {
        LOG.info(
                "start buffer data, read queue size {}, write queue size {}, buffer cache size {}, buffer pool size {}",
                readQueue.size(),
                writeQueue.size(),
                bufferCache.size(),
                bufferPool.size());
        try {
            // if the cache have data, that should be restarted from previous error
            if (currentReadBuffer != null && currentReadBuffer.limit() != 0) {
                currentReadBuffer.rewind();
                readQueue.putFirst(currentReadBuffer);
                currentReadBuffer = null;
            }
            // re-read the data in bufferCache
            ByteBuffer buffer = bufferCache.pollFirst();
            while (buffer != null) {
                buffer.rewind();
                readQueue.putFirst(buffer);
                buffer = bufferCache.pollFirst();
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int read(byte[] buf) throws InterruptedException {
        if (currentReadBuffer == null) {
            currentReadBuffer = readQueue.take();
        }
        // add empty buffer as end flag
        if (currentReadBuffer.limit() == 0) {
            Preconditions.checkState(readQueue.size() == 0);
            bufferCache.putFirst(currentReadBuffer);
            writeQueue.offer(allocate());
            currentReadBuffer = null;
            return -1;
        }

        int available = currentReadBuffer.remaining();
        int nRead = Math.min(available, buf.length);
        currentReadBuffer.get(buf, 0, nRead);
        if (currentReadBuffer.remaining() == 0) {
            bufferCache.putFirst(currentReadBuffer);
            writeQueue.offer(allocate());
            currentReadBuffer = null;
        }
        return nRead;
    }

    public void recycleCache() {
        // recycle cache buffer
        Preconditions.checkState(readQueue.size() == 0);
        ByteBuffer buff = bufferCache.poll();
        while (buff != null) {
            buff.clear();
            bufferPool.add(buff);
            buff = bufferCache.poll();
        }
    }

    private ByteBuffer allocate() {
        ByteBuffer buff = bufferPool.poll();
        return buff != null ? buff : ByteBuffer.allocate(bufferCapacity);
    }

    @VisibleForTesting
    public int getBufferCacheSize() {
        return bufferCache.size();
    }

    @VisibleForTesting
    public int getBufferPoolSize() {
        return bufferPool.size();
    }
}
