package org.apache.doris.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverterConfig;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.DateToStringConverter;
import org.apache.doris.flink.sink.writer.JsonDebeziumSchemaSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class MysqlCDCSource1 {

    public static void main(String[] args) throws Exception {

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(false, customConverterConfigs);

//        Properties properties = new Properties();
//        properties.setProperty("converters", "date");
//        properties.setProperty("date.type", "org.apache.doris.flink.sink.writer.DateToStringConverter");
//        properties.setProperty("date.format.date","yyyy-MM-dd");
//        properties.setProperty("date.format.datetime","yyyy-MM-dd HH:mm:ss");
//        properties.setProperty("date.format.timestamp","yyyy-MM-dd HH:mm:ss");
//        properties.setProperty("date.format.timestamp.zone", "UTC");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .databaseList("test") // set captured database
                .tableList("test.*") // set captured table
                .username("root")
                .password("123456")
                .debeziumProperties(DateToStringConverter.DEFAULT_PROPS)
                .deserializer(schema)
                .serverTimeZone("Asia/Shanghai")
                .includeSchemaChanges(true) // converts SourceRecord to JSON String
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(5000);
//
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("47.109.38.38:8030")
                .setTableIdentifier("test.test")
                .setUsername("root")
                .setPassword("").build();
//
        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris" + UUID.randomUUID())
                .setStreamLoadProp(props).setDeletable(true);

        DorisSink.Builder<String> builder = DorisSink.builder();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setDorisOptions(dorisOptions)
                .setSerializer(JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisOptions).build());

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")//.print();
                .sinkTo(builder.build());

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
