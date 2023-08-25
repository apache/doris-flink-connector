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
