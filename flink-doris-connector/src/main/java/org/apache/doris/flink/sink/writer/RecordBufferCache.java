package org.apache.doris.flink.sink.writer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class RecordBufferCache extends InputStream {

    private List<ByteBuffer> recordBuffers;
    private int buffSize;
    private ByteBuffer currentWriteBuffer;
    private ByteBuffer currentReadBuffer;
    private boolean loadBatchFirstRecord;
    private volatile int curReadInd;

    public RecordBufferCache(int bufferSize) {
        buffSize = bufferSize;
        recordBuffers = new LinkedList<ByteBuffer>();
        currentWriteBuffer = null;
        loadBatchFirstRecord = true;
        curReadInd = 0;
        currentReadBuffer = null;
    }

    public void recycle() {
        for(ByteBuffer buff: recordBuffers) {
            buff.clear();
            ByteBufferManager.getByteBufferManager().recycle(buff);
        }

        currentWriteBuffer.clear();
        ByteBufferManager.getByteBufferManager().recycle(currentWriteBuffer);
    }

    public void startInput() {
        // if the cache have data, that should be restart from previous error
        // reset the position for each record buffer
        for (ByteBuffer buff: recordBuffers) {
            buff.position(0);
        }
    }

    public void endInput() throws IOException {
        try {
            // add Empty buffer as finish flag.
            boolean isEmpty = false;
            if (currentWriteBuffer != null) {
                currentWriteBuffer.flip();
                // check if the current write buffer is empty.
                isEmpty = currentWriteBuffer.limit() == 0;
                recordBuffers.add(currentWriteBuffer);
                currentWriteBuffer = null;
            }
            if (!isEmpty) {
                ByteBuffer byteBuffer = ByteBufferManager.getByteBufferManager().allocate(buffSize);
                byteBuffer.flip();
                Preconditions.checkState(byteBuffer.limit() == 0);
                recordBuffers.add(byteBuffer);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public int read() throws IOException {
        return 0;
    }

    @Override
    public int read(byte[] buf) throws IOException {
        if (recordBuffers.size()==0 || recordBuffers.size() <= curReadInd) {
            // waiting for new data
            try {
                wait();
            } catch (InterruptedException e) {
                // get new buffer
            }
        }

        if(currentReadBuffer == null) {
            currentReadBuffer = recordBuffers.get(curReadInd);
            curReadInd++;
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
                recordBuffers.add(currentWriteBuffer);
                // notify have new buffer for reading
                notify();
                currentWriteBuffer = null;
            }
        } while (wPos != buf.length);
    }

    public void writeRecord(byte[] buf, byte[] lineDelimiter) {
        if (loadBatchFirstRecord) {
            loadBatchFirstRecord = false;
        } else {
            write(lineDelimiter);
        }
        write(buf);
    }

    public List<ByteBuffer> getRecordBuffers() {
        return this.recordBuffers;
    }

    public ByteBuffer getCurrentWriteBuffer() {
        return this.currentWriteBuffer;
    }
}
