package org.apache.doris.flink.deserialization.convert;

import org.apache.doris.flink.deserialization.converter.DorisRowConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

public class DorisRowConverterTest implements Serializable {

    @Test
    public void testConvert(){
        ResolvedSchema SCHEMA =
                ResolvedSchema.of(
                        Column.physical("f1", DataTypes.NULL()),
                        Column.physical("f2", DataTypes.BOOLEAN()),
                        Column.physical("f3", DataTypes.FLOAT()),
                        Column.physical("f4", DataTypes.DOUBLE()),
                        Column.physical("f5", DataTypes.INTERVAL(DataTypes.YEAR())),
                        Column.physical("f6", DataTypes.INTERVAL(DataTypes.DAY())),
                        Column.physical("f7", DataTypes.TINYINT()),
                        Column.physical("f8", DataTypes.SMALLINT()),
                        Column.physical("f9", DataTypes.INT()),
                        Column.physical("f10", DataTypes.BIGINT()),
                        Column.physical("f11", DataTypes.DECIMAL(10,2)),
                        Column.physical("f12", DataTypes.TIMESTAMP_WITH_TIME_ZONE()),
                        Column.physical("f13", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()),
                        Column.physical("f14", DataTypes.DATE()),
                        Column.physical("f15", DataTypes.CHAR(1)),
                        Column.physical("f16", DataTypes.VARCHAR(256)));

        DorisRowConverter converter = new DorisRowConverter((RowType) SCHEMA.toPhysicalRowDataType().getLogicalType());

        List record = Arrays.asList(null,"true",1.2,1.2345,24,10,1,32,64,128, BigDecimal.valueOf(10.123),"2021-01-01 08:00:00","2021-01-01 08:00:00","2021-01-01","a","doris");
        GenericRowData rowData = converter.convert(record);
        Assert.assertEquals("+I(null,true,1.2,1.2345,24,10,1,32,64,128,10.12,2021-01-01 08:00:00,2021-01-01 08:00:00,2021-01-01,a,doris)",rowData.toString());
    }
}
