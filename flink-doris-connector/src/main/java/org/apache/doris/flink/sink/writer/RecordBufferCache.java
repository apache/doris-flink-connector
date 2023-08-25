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

import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingDeque;

public class RecordBufferCache extends InputStream {
    LinkedBlockingDeque<ByteBuffer> readQueue;
    LinkedBlockingDeque<ByteBuffer> cacheQueue;
    private int buffSize;
    private ByteBuffer currentWriteBuffer;
    private ByteBuffer currentReadBuffer;

    public RecordBufferCache(int bufferSize) {
        buffSize = bufferSize;
        readQueue = new LinkedBlockingDeque<>();
        cacheQueue = new LinkedBlockingDeque<>();
    }

    public void recycle() {
        Preconditions.checkState(readQueue.poll() == null);
        ByteBuffer buff = cacheQueue.poll();
        while (buff != null) {
            buff.clear();
            ByteBufferManager.getByteBufferManager().recycle(buff);
            buff = cacheQueue.poll();
        }
    }

    public void startInput() {
        // if the cache have data, that should be restarted from previous error
        // re-read the data in cacheQueue
        if (currentReadBuffer != null) {
            currentReadBuffer.rewind();
            readQueue.addFirst(currentReadBuffer);
            currentReadBuffer = null;
        }
        ByteBuffer buffer = cacheQueue.pollLast();
        while (buffer != null) {
            buffer.rewind();
            readQueue.addFirst(buffer);
            buffer = cacheQueue.pollLast();
        }
    }

    public void endInput() {
        // add Empty buffer as finish flag.
        boolean isEmpty = false;
        if (currentWriteBuffer != null) {
            currentWriteBuffer.flip();
            // check if the current write buffer is empty.
            isEmpty = currentWriteBuffer.limit() == 0;
            try {
                readQueue.put(currentWriteBuffer);
            } catch (InterruptedException e) {
                throw new DorisRuntimeException(e);
            }
            currentWriteBuffer = null;
        }
        if (!isEmpty) {
            ByteBuffer byteBuffer = ByteBufferManager.getByteBufferManager().allocate(buffSize);
            byteBuffer.flip();
            Preconditions.checkState(byteBuffer.limit() == 0);
            readQueue.add(byteBuffer);
        }
    }

    @Override
    public int read() throws IOException {
        return 0;
    }

    @Override
    public int read(byte[] buf) throws IOException{
        try {
            if (currentReadBuffer == null) {
                currentReadBuffer = readQueue.take();
            }
            // add empty buffer as end flag
            if (currentReadBuffer.limit() == 0) {
                cacheQueue.put(currentReadBuffer);
                currentReadBuffer = null;
                return -1;
            }

            int available = currentReadBuffer.remaining();
            int nRead = Math.min(available, buf.length);
            currentReadBuffer.get(buf, 0, nRead);
            if (currentReadBuffer.remaining() == 0) {
                cacheQueue.put(currentReadBuffer);
                currentReadBuffer = null;
            }
            return nRead;
        } catch (InterruptedException e) {
            throw new DorisRuntimeException(e);
        }
    }

    public void write(byte[] buf) {
        int wPos = 0;
        do {
            if (currentWriteBuffer == null) {
                currentWriteBuffer = ByteBufferManager.getByteBufferManager().allocate(buffSize);
            }
            int available = currentWriteBuffer.remaining();
            int nWrite = Math.min(available, buf.length - wPos);
            currentWriteBuffer.put(buf, wPos, nWrite);
            wPos += nWrite;
            if (currentWriteBuffer.remaining() == 0) {
                currentWriteBuffer.flip();
                try {
                    readQueue.put(currentWriteBuffer);
                } catch (InterruptedException e) {
                    throw new DorisRuntimeException(e);
                }
                currentWriteBuffer = null;
            }
        } while (wPos != buf.length);
    }
}
