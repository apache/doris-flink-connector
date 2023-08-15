package org.apache.doris.flink.sink.writer;

import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RecordBufferCache extends InputStream {
    private static final Logger LOG = LoggerFactory.getLogger(RecordBufferCache.class);
    private List<ByteBuffer> recordBuffers;
    private int buffSize;
    private ByteBuffer currentWriteBuffer;
    private ByteBuffer currentReadBuffer;
    private volatile int curReadInd;

    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    public RecordBufferCache(int bufferSize) {
        buffSize = bufferSize;
        recordBuffers = new LinkedList<>();
        currentWriteBuffer = null;
        curReadInd = 0;
        currentReadBuffer = null;
    }

    public void recycle() {
        for(ByteBuffer buff: recordBuffers) {
            buff.clear();
            ByteBufferManager.getByteBufferManager().recycle(buff);
        }
    }

    public void startInput() {
        // if the cache have data, that should be restarted from previous error
        // reset the position for each record buffer
        for (ByteBuffer buff: recordBuffers) {
            buff.rewind();
        }
        curReadInd = 0;
        currentReadBuffer = null;
    }

    public void endInput() throws IOException {
        // add Empty buffer as finish flag.
        boolean isEmpty = false;
        if (currentWriteBuffer != null) {
            currentWriteBuffer.flip();
            // check if the current write buffer is empty.
            isEmpty = currentWriteBuffer.limit() == 0;
            lock.lock();
            try {
                recordBuffers.add(currentWriteBuffer);
                condition.signal();
            } finally {
                lock.unlock();
            }
            currentWriteBuffer = null;
        }
        if (!isEmpty) {
            ByteBuffer byteBuffer = ByteBufferManager.getByteBufferManager().allocate(buffSize);
            byteBuffer.flip();
            Preconditions.checkState(byteBuffer.limit() == 0);
            lock.lock();
            try {
                recordBuffers.add(byteBuffer);
                condition.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public int read() throws IOException {
        return 0;
    }

    @Override
    public int read(byte[] buf) throws IOException {
        lock.lock();
        try {
            if (recordBuffers.isEmpty() || recordBuffers.size() <= curReadInd) {
                condition.await();
            }
            if(currentReadBuffer == null) {
                currentReadBuffer = recordBuffers.get(curReadInd);
                curReadInd++;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        // add empty buffer as end flag
        if (currentReadBuffer.limit() == 0) {
            currentReadBuffer = null;
            return -1;
        }

        int available = currentReadBuffer.remaining();
        int nRead = Math.min(available, buf.length);
        currentReadBuffer.get(buf, 0, nRead);
        if (currentReadBuffer.remaining() == 0) {
            currentReadBuffer = null;
        }

        return nRead;
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
                lock.lock();
                try {
                    recordBuffers.add(currentWriteBuffer);
                    condition.signal();
                } finally {
                    lock.unlock();
                }

                currentWriteBuffer = null;
            }
        } while (wPos != buf.length);
    }
}
