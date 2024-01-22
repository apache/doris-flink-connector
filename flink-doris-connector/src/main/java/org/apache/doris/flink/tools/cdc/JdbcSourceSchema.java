package org.apache.doris.flink.tools.cdc;

import org.apache.doris.flink.catalog.doris.FieldSchema;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public abstract class JdbcSourceSchema extends SourceSchema {

    public JdbcSourceSchema(
            DatabaseMetaData metaData,
            String databaseName,
            String schemaName,
            String tableName,
            String tableComment)
            throws Exception {
        super(databaseName, schemaName, tableName, tableComment);
        fields = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getColumns(databaseName, schemaName, tableName, null)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                String comment = rs.getString("REMARKS");
                String fieldType = rs.getString("TYPE_NAME");
                Integer precision = rs.getInt("COLUMN_SIZE");

                if (rs.wasNull()) {
                    precision = null;
                }
                Integer scale = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    scale = null;
                }
                String dorisTypeStr = convertToDorisType(fieldType, precision, scale);
                fields.put(fieldName, new FieldSchema(fieldName, dorisTypeStr, comment));
            }
        }

        primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, schemaName, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        }
    }

    public abstract String convertToDorisType(String fieldType, Integer precision, Integer scale);
}
