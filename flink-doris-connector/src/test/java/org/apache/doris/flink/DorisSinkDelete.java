package org.apache.doris.flink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.RowDataSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Properties;
import java.util.UUID;

public class DorisSinkDelete {

    public static void main(String[] args) throws Exception {

        long checkpoingTime = 10000;
        int batchSize = 10;
        String fenode = "47.109.38.38:8030";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(checkpoingTime);
        env.setParallelism(1);
        DorisSink.Builder<RowData> builder = DorisSink.builder();
        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes(fenode)
                .setTableIdentifier("test.test")
                .setUsername("root")
                .setPassword("");

        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
//        properties.setProperty("columns", "id,name,__DORIS_DELETE_SIGN__");
        executionBuilder.setLabelPrefix("doris-label" + UUID.randomUUID()).setDeletable(true)
                .setStreamLoadProp(properties);

        builder.setDorisReadOptions(readOptionBuilder.build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(RowDataSerializer.builder()
                        .setFieldNames(new String[]{"id","name"})
                        .setType("json").enableDelete(true)
                        .setFieldType(new DataType[]{DataTypes.INT(),DataTypes.VARCHAR(200)}).build())
                .setDorisOptions(dorisBuilder.build());


        DataStreamSource<RowData> source = env.addSource(new SourceFunction<RowData>() {
            private int i = 0;
            @Override
            public void run(SourceContext<RowData> sourceContext) throws Exception {
                while(true){
                    GenericRowData genericRowData = new GenericRowData(2);
                    genericRowData.setRowKind(RowKind.INSERT);
                    genericRowData.setField(0, i);
                    genericRowData.setField(1, StringData.fromString("zhangsan" + i));

                    if(i % 10 == 0){
                        genericRowData.setRowKind(RowKind.DELETE);
                    }
                    if(i % 11 == 0){
                        genericRowData.setField(0, 1);
                        genericRowData.setField(1, StringData.fromString("zhangsan" + i));
                        genericRowData.setRowKind(RowKind.UPDATE_AFTER);
                    }
                    sourceContext.collect(genericRowData);
                    Thread.sleep(1000);
                    ++i;
                }
            }
            @Override
            public void cancel() {
            }
        });

        source.sinkTo(builder.build());

        env.execute("doris test");
    }
}
