package org.apache.doris.flink.sink.writer;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class TestRecordBuffer {

    @Test
    public void testStopBufferData() throws Exception {
        RecordBuffer recordBuffer = new RecordBuffer(16, 3);
        recordBuffer.stopBufferData();
        Assert.assertEquals(1, recordBuffer.getReadQueueSize());
        Assert.assertEquals(2, recordBuffer.getWriteQueueSize());

        recordBuffer = new RecordBuffer(16, 3);
        recordBuffer.write("test".getBytes());
        recordBuffer.stopBufferData();
        Assert.assertEquals(2, recordBuffer.getReadQueueSize());
        Assert.assertEquals(1, recordBuffer.getWriteQueueSize());
    }

    @Test
    public void testWrite() throws Exception {
        RecordBuffer recordBuffer = new RecordBuffer(16, 3);
        recordBuffer.startBufferData();
        recordBuffer.write("This is Test".getBytes());
        Assert.assertEquals(0, recordBuffer.getReadQueueSize());
        Assert.assertEquals(2, recordBuffer.getWriteQueueSize());
        recordBuffer.write(" for RecordBuffer".getBytes());
        Assert.assertEquals(1, recordBuffer.getReadQueueSize());
        Assert.assertEquals(1, recordBuffer.getWriteQueueSize());
    }

    @Test
    public void testRead() throws Exception {
        RecordBuffer recordBuffer = new RecordBuffer(16, 3);
        recordBuffer.startBufferData();
        recordBuffer.write("This is Test for RecordBuffer!".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(1, recordBuffer.getReadQueueSize());
        Assert.assertEquals(1, recordBuffer.getWriteQueueSize());
        byte[] buffer = new byte[16];
        int nRead = recordBuffer.read(buffer);
        Assert.assertEquals(0, recordBuffer.getReadQueueSize());
        Assert.assertEquals(2, recordBuffer.getWriteQueueSize());
        Assert.assertEquals(16, nRead);
        Assert.assertArrayEquals("This is Test for".getBytes(StandardCharsets.UTF_8), buffer);

        recordBuffer.write("Continue to write the last one.".getBytes(StandardCharsets.UTF_8));
        buffer = new byte[7];
        nRead = recordBuffer.read(buffer);
        Assert.assertEquals(7, nRead);
        Assert.assertArrayEquals(" Record".getBytes(StandardCharsets.UTF_8), buffer);
        Assert.assertEquals(1, recordBuffer.getReadQueueSize());
        Assert.assertEquals(0, recordBuffer.getWriteQueueSize());
    }
}
