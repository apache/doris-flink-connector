package org.apache.doris.flink.source.reader;

import java.util.List;

public interface ValueReader {
    boolean hasNext();

    List next();

    void close() throws Exception;
}
