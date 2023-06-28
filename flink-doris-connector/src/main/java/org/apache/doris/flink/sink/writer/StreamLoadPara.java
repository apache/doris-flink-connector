package org.apache.doris.flink.sink.writer;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;

public class StreamLoadPara {

    public long lastCheckpointId;
    public String labelPrefix;
    public DorisOptions dorisOptions;
    public DorisReadOptions dorisReadOptions;
    public DorisExecutionOptions executionOptions;

    public StreamLoadPara() {

    }

}
