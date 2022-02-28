package org.apache.doris.flink.sink;

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static org.apache.doris.flink.sink.writer.LoadConstants.FIELD_DELIMITER_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

public class TestEscapeHandler {
    @Test
    public void testHandle() {
        Properties properties = new Properties();
        properties.setProperty(FIELD_DELIMITER_KEY, "\\x09\\x09");
        properties.setProperty(LINE_DELIMITER_KEY, "\\x0A\\x0A");
        EscapeHandler.handleEscape(properties);
        Assert.assertEquals("\t\t", properties.getProperty(FIELD_DELIMITER_KEY));
        Assert.assertEquals("\n\n", properties.getProperty(LINE_DELIMITER_KEY));
    }
}
