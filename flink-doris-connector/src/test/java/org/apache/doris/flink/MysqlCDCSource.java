package org.apache.doris.flink;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.RowDataSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Properties;

public class MysqlCDCSource {

    public static void main(String[] args) throws Exception {

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("69.230.245.132")
                .port(3306)
                .databaseList("test") // set captured database
                .tableList("test.test") // set captured table
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .includeSchemaChanges(true)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(10000);

        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");

        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("161.189.169.254:8030")
                .setTableIdentifier("test.test")
                .setUsername("root")
                .setPassword("");

        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris1")
                .setStreamLoadProp(properties);
//
        DorisSink.Builder<RowData> builder = DorisSink.builder();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(RowDataSerializer.builder()
                        .setFieldType(new DataType[]{DataTypes.INT(),DataTypes.VARCHAR(244)})
                        .setType("json")
                        .setFieldNames(new String[]{"id","name"}).build())
                .setDorisOptions(dorisBuilder.build());

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")//.print();
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String json) throws Exception {
                        //过滤ddl语句->后续调用schema change api
                        JSONObject jsob = JSONObject.parseObject(json);
                        String op = jsob.getString("op");
                        System.out.println("---" + json);
                        return StringUtils.isNotEmpty(op) && !op.equals("r");
                    }
                })
           .map(new MapFunction<String, RowData>() {
               @Override
               public RowData map(String json) throws Exception {
                   JSONObject jsob = JSONObject.parseObject(json);
                   String op = jsob.getString("op");

                   JSONObject data = new JSONObject();
                   //将数据解析位Rowdata
                   RowKind rowKind = null;
                   if(op.equals("c")){
                        //insert
                        rowKind = RowKind.INSERT;
                        data = jsob.getJSONObject("after");
                    }else if(op.equals("u")){
                       //update
                       rowKind = RowKind.UPDATE_AFTER;
                       data = jsob.getJSONObject("after");
                   }else if(op.equals("d")){
                       rowKind = RowKind.DELETE;
                       data = jsob.getJSONObject("before");
                   }
                   GenericRowData genericRowData = new GenericRowData(2);
                   genericRowData.setRowKind(rowKind);
                   genericRowData.setField(0,data.get("id"));
                   genericRowData.setField(1, StringData.fromString(data.getString("name")));
                   return genericRowData;
               }
           }) //.print();
            .sinkTo(builder.build());

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
