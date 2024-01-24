package org.apache.doris.flink.tools.cdc.db2;

import org.apache.doris.flink.tools.cdc.SourceSchema;

import java.sql.DatabaseMetaData;

public class Db2Schema extends SourceSchema {
    public Db2Schema(
            DatabaseMetaData metaData,
            String databaseName,
            String schemaName,
            String tableName,
            String tableComment)
            throws Exception {
        super(metaData,null, schemaName, tableName, tableComment);
    }

    @Override
    public String convertToDorisType(String fieldType, Integer precision, Integer scale) {
        return Db2Type.toDorisType(fieldType, precision, scale);
    }
}
