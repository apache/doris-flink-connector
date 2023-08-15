package org.apache.doris.flink.sink.writer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

public class ByteBufferManager {

    private HashMap<Integer,LinkedList<ByteBuffer>> bufferList;

    private static class SingletonHolder {
        private static final ByteBufferManager INSTANCE = new ByteBufferManager();
    }

    public static ByteBufferManager getByteBufferManager() {
        return SingletonHolder.INSTANCE;
    }

    private ByteBufferManager() {
        this.bufferList = new HashMap<>();
    }

    public ByteBuffer allocate(int bufferSize) {
        if(!bufferList.containsKey(bufferSize)) {
            bufferList.put(bufferSize, new LinkedList<>());
        }
        if(bufferList.get(bufferSize).size() > 0) {
            return bufferList.get(bufferSize).removeFirst();
        } else {
            return ByteBuffer.allocate(bufferSize);
        }
    }

    public void recycle(ByteBuffer buff) {
        if(!bufferList.containsKey(buff.capacity())) {
            bufferList.put(buff.capacity(), new LinkedList<>());
        }
        bufferList.get(buff.capacity()).addLast(buff);
    }
}
