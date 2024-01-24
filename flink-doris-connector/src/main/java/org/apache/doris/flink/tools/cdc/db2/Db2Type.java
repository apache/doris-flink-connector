package org.apache.doris.flink.tools.cdc.db2;

import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.catalog.doris.DorisType;

public class Db2Type {
    private static final String SMALLINT = "SMALLINT";
    private static final String INTEGER = "INTEGER";
    private static final String INT = "INT";
    private static final String BIGINT = "BIGINT";
    private static final String REAL = "REAL";
    private static final String DOUBLE = "DOUBLE";
    private static final String DECIMAL = "DECIMAL";
    private static final String NUMERIC = "NUMERIC";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String CHARACTER = "CHARACTER";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String BLOB = "BLOB";
    private static final String CLOB = "CLOB";
    private static final String DBCLOB = "DBCLOB";
    private static final String XML = "XML";
    private static final String VARGRAPHIC = "VARGRAPHIC";
    private static final String GRAPHIC = "GRAPHIC";

    public static String toDorisType(String db2Type, Integer precision, Integer scale) {
        db2Type = db2Type.toUpperCase();
        switch (db2Type) {
            case SMALLINT:
                return DorisType.SMALLINT;
            case INTEGER:
            case INT:
                return DorisType.INT;
            case BIGINT:
                return DorisType.BIGINT;
            case REAL:
                return DorisType.FLOAT;
            case DOUBLE:
                return DorisType.DOUBLE;
            case DATE:
                return DorisType.DATE_V2;
            case DECIMAL:
            case NUMERIC:
                if (precision != null && precision > 0 && precision <= 38) {
                    if (scale != null && scale >= 0) {
                        return String.format("%s(%s,%s)", DorisType.DECIMAL_V3, precision, scale);
                    }
                    return String.format("%s(%s,%s)", DorisType.DECIMAL_V3, precision, 0);
                } else {
                    return DorisType.STRING;
                }
            case CHARACTER:
            case TIME:
            case CHAR:
            case VARCHAR:
                Preconditions.checkNotNull(precision);
                return precision * 3 > 65533
                        ? DorisType.STRING
                        : String.format("%s(%s)", DorisType.VARCHAR, precision * 3);
            case TIMESTAMP:
                return String.format(
                        "%s(%s)", DorisType.DATETIME_V2, Math.min(scale == null ? 0 : scale, 6));
            case XML:
            case VARGRAPHIC:
            case GRAPHIC:
            case BLOB:
            case CLOB:
            case DBCLOB:
                return DorisType.STRING;
            case BINARY:
            case VARBINARY:
            default:
                throw new UnsupportedOperationException("Unsupported DB2 Type: " + db2Type);
        }
    }
}
