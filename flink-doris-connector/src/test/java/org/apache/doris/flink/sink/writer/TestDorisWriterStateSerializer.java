package org.apache.doris.flink.sink.writer;

import org.junit.Assert;
import org.junit.Test;

public class TestDorisWriterStateSerializer {
    @Test
    public void testSerialize() throws Exception {
        DorisWriterState expectDorisWriterState = new DorisWriterState("doris");
        DorisWriterStateSerializer serializer = new DorisWriterStateSerializer();
        DorisWriterState dorisWriterState =  serializer.deserialize(1, serializer.serialize(expectDorisWriterState));
        Assert.assertEquals(expectDorisWriterState, dorisWriterState);
    }
}
