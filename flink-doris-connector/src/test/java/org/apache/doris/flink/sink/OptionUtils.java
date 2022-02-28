package org.apache.doris.flink.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.flink.configuration.ExecutionOptions;

import java.util.Properties;

public class OptionUtils {
    public static DorisExecutionOptions buildExecutionOptional() {
        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");

        DorisExecutionOptions.Builder builder = DorisExecutionOptions.builder();
        builder.setLabelPrefix("doris")
                .setStreamLoadProp(properties)
                .setBufferSize(8*1024)
                .setBufferCount(3)
                .setDeletable(true)
                .setCheckInterval(100)
                .setMaxRetries(2);
        return builder.build();
    }
    public static DorisExecutionOptions buildExecutionOptional(Properties properties) {

        DorisExecutionOptions.Builder builder = DorisExecutionOptions.builder();
        builder.setLabelPrefix("doris")
                .setStreamLoadProp(properties)
                .setBufferSize(8*1024)
                .setBufferCount(3)
                .setDeletable(true)
                .setCheckInterval(100)
                .setMaxRetries(2);
        return builder.build();
    }

    public static DorisReadOptions buildDorisReadOptions() {
        DorisReadOptions.Builder builder = DorisReadOptions.builder();
        builder.setDeserializeArrowAsync(false)
                .setDeserializeQueueSize(64)
                .setExecMemLimit(2147483648L)
                .setRequestQueryTimeoutS(3600)
                .setRequestBatchSize(1000)
                .setRequestConnectTimeoutMs(10000)
                .setRequestReadTimeoutMs(10000)
                .setRequestRetries(3)
                .setRequestTabletSize(1024 * 1024);
        return builder.build();
    }

    public static DorisOptions buildDorisOptions() {
        DorisOptions.Builder builder = DorisOptions.builder();
        builder.setFenodes("local:8040")
                .setTableIdentifier("db_test.table_test")
                .setUsername("u_test")
                .setPassword("p_test");
        return builder.build();
    }
}
