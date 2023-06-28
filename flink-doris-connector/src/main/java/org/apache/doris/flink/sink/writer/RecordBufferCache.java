package org.apache.doris.flink.sink.writer;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class RecordBufferCache {

    private List<ByteBuffer> recordBuffers;
    private int buffSize;
    private ByteBuffer currentWriteBuffer;
    private boolean loadBatchFirstRecord;

    public RecordBufferCache(int bufferSize) {
        buffSize = bufferSize;
        recordBuffers = new LinkedList<ByteBuffer>();
        currentWriteBuffer = null;
        loadBatchFirstRecord = true;
    }

    public void recycle() {
        for(ByteBuffer buff: recordBuffers) {
            buff.clear();
            ByteBufferManager.getByteBufferManager().recycle(buff);
        }

        currentWriteBuffer.clear();
        ByteBufferManager.getByteBufferManager().recycle(currentWriteBuffer);
    }

    public void write(byte[] buf) {
        int wPos = 0;
        do {
            if (currentWriteBuffer == null) {
                // TODO get bytebuffer from cache
                currentWriteBuffer = ByteBufferManager.getByteBufferManager().allocate(buffSize);
            }
            int available = currentWriteBuffer.remaining();
            int nWrite = Math.min(available, buf.length - wPos);
            currentWriteBuffer.put(buf, wPos, nWrite);
            wPos += nWrite;
            if (currentWriteBuffer.remaining() == 0) {
                currentWriteBuffer.flip();
                recordBuffers.add(currentWriteBuffer);
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

    public RecordBufferCache deepCopy() {
        RecordBufferCache copy = new RecordBufferCache(buffSize);
        for(ByteBuffer buff: this.recordBuffers) {
            copy.recordBuffers.add(deepCopy(buff));
        }
        copy.currentWriteBuffer = deepCopy(this.currentWriteBuffer);
        copy.loadBatchFirstRecord = this .loadBatchFirstRecord;

        return copy;
    }

    private ByteBuffer deepCopy(ByteBuffer orig) {
        int pos = orig.position(), lim = orig.limit();
        try {
            orig.position(0).limit(orig.capacity()); // set range to entire buffer
            ByteBuffer toReturn = deepCopyVisible(orig); // deep copy range
            toReturn.position(pos).limit(lim); // set range to original
            return toReturn;
        } finally { // do in finally in case something goes wrong we don't bork the orig

            orig.position(pos).limit(lim); // restore original
        }
    }

    private ByteBuffer deepCopyVisible(ByteBuffer orig) {
        int pos = orig.position();
        try {
            ByteBuffer toReturn;
            toReturn = ByteBufferManager.getByteBufferManager().allocate(orig.remaining());

            toReturn.put(orig);
            toReturn.order(orig.order());

            return (ByteBuffer) toReturn.position(0);
        } finally {
            orig.position(pos);
        }
    }
}
