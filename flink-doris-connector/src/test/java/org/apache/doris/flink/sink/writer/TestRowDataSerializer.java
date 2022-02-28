package org.apache.doris.flink.sink.writer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TestRowDataSerializer {

    static GenericRowData rowData;
    static DataType[] dataTypes;
    static String[] fieldNames;
    @BeforeClass
    public static void setUp() {
        rowData = new GenericRowData(3);
        rowData.setField(0, 3);
        rowData.setField(1, StringData.fromString("test"));
        rowData.setField(2, 60.2);
        rowData.setRowKind(RowKind.INSERT);
        dataTypes = new DataType[]{DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()};
        fieldNames = new String[]{"id", "name","weight"};
    }
    @Test
    public void testSerializeCsv() throws IOException {
        RowDataSerializer.Builder builder = RowDataSerializer.builder();
        builder.setFieldNames(fieldNames).setFieldType(dataTypes).setType("csv").setFieldDelimiter("|").enableDelete(false);
        RowDataSerializer serializer = builder.build();
        byte[] serializedValue = serializer.serialize(rowData);
        Assert.assertArrayEquals("3|test|60.2".getBytes(StandardCharsets.UTF_8), serializedValue);
    }

    @Test
    public void testSerializeJson() throws IOException {
        RowDataSerializer.Builder builder = RowDataSerializer.builder();
        builder.setFieldNames(fieldNames).setFieldType(dataTypes).setType("json").setFieldDelimiter("|").enableDelete(false);
        RowDataSerializer serializer = builder.build();
        byte[] serializedValue = serializer.serialize(rowData);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> valueMap = objectMapper.readValue(new String(serializedValue, StandardCharsets.UTF_8), new TypeReference<Map<String, String>>(){});
        Assert.assertEquals("3", valueMap.get("id"));
        Assert.assertEquals("test", valueMap.get("name"));
        Assert.assertEquals("60.2", valueMap.get("weight"));
    }

    @Test
    public void testSerializeCsvWithSign() throws IOException {
        RowDataSerializer.Builder builder = RowDataSerializer.builder();
        builder.setFieldNames(fieldNames).setFieldType(dataTypes).setType("csv").setFieldDelimiter("|").enableDelete(true);
        RowDataSerializer serializer = builder.build();
        byte[] serializedValue = serializer.serialize(rowData);
        Assert.assertArrayEquals("3|test|60.2|0".getBytes(StandardCharsets.UTF_8), serializedValue);
    }

    @Test
    public void testSerializeJsonWithSign() throws IOException {
        RowDataSerializer.Builder builder = RowDataSerializer.builder();
        builder.setFieldNames(fieldNames).setFieldType(dataTypes).setType("json").setFieldDelimiter("|").enableDelete(true);
        RowDataSerializer serializer = builder.build();
        byte[] serializedValue = serializer.serialize(rowData);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> valueMap = objectMapper.readValue(new String(serializedValue, StandardCharsets.UTF_8), new TypeReference<Map<String, String>>(){});
        Assert.assertEquals("3", valueMap.get("id"));
        Assert.assertEquals("test", valueMap.get("name"));
        Assert.assertEquals("60.2", valueMap.get("weight"));
        Assert.assertEquals("0", valueMap.get("__DORIS_DELETE_SIGN__"));

    }

    @Test
    public void testParseDeleteSign() {
        RowDataSerializer.Builder builder = RowDataSerializer.builder();
        builder.setFieldNames(fieldNames).setFieldType(dataTypes).setType("json").setFieldDelimiter("|").enableDelete(true);
        RowDataSerializer serializer = builder.build();
        Assert.assertEquals("0", serializer.parseDeleteSign(RowKind.INSERT));
        Assert.assertEquals("0", serializer.parseDeleteSign(RowKind.UPDATE_AFTER));
        Assert.assertEquals("1", serializer.parseDeleteSign(RowKind.DELETE));
        Assert.assertEquals("1", serializer.parseDeleteSign(RowKind.UPDATE_BEFORE));
    }
}
