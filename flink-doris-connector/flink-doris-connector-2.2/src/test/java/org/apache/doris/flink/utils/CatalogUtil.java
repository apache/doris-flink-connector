package org.apache.doris.flink.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.ArrayList;
import java.util.Map;

public class CatalogUtil {
    public static CatalogTable createTable(Schema tableSchema, Map<String, String> options) {
        return CatalogTable.newBuilder()
                .comment("FlinkTable")
                .partitionKeys(new ArrayList<>())
                .schema(tableSchema)
                .options(options)
                .build();
    }

    public static Schema getTableSchema() {
        return Schema.newBuilder()
                .column("id", new AtomicDataType(new VarCharType(false, 128)))
                .column("c_boolean", DataTypes.BOOLEAN())
                .column("c_char", DataTypes.CHAR(1))
                .column("c_date", DataTypes.DATE())
                .column("c_datetime", DataTypes.TIMESTAMP(0))
                .column("c_decimal", DataTypes.DECIMAL(10, 2))
                .column("c_double", DataTypes.DOUBLE())
                .column("c_float", DataTypes.FLOAT())
                .column("c_int", DataTypes.INT())
                .column("c_bigint", DataTypes.BIGINT())
                .column("c_largeint", DataTypes.STRING())
                .column("c_smallint", DataTypes.SMALLINT())
                .column("c_string", DataTypes.STRING())
                .column("c_tinyint", DataTypes.TINYINT())
                .column("c_array", DataTypes.ARRAY(DataTypes.INT()))
                .column("c_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                .column("c_row", DataTypes.ROW())
                .column("c_varbinary", DataTypes.VARBINARY(16))
                .primaryKey("id")
                .build();
    }

    public static String[] getColumns() {
        return getTableSchema().getColumns().stream()
                .map(UnresolvedColumn::getName)
                .toArray(String[]::new);
    }
}
