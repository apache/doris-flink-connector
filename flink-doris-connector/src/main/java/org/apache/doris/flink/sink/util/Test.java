package org.apache.doris.flink.sink.util;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;

public class Test {

    public static void main(String[] args) {
        String sql =
                "ALTER TABLE connect.out_bound_test ADD COLUMN `other_line_no_1` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '' COMMENT '第三方单行号1号'";

        try {
            // 解析 SQL 语句
            Statement statement = CCJSqlParserUtil.parse(sql);

            // 检查是否是 ALTER TABLE 语句
            if (statement instanceof Alter) {
                Alter alterStatement = (Alter) statement;

                // 获取表信息
                Table table = alterStatement.getTable();
                System.out.println("Table: " + table.getFullyQualifiedName());

                //                // 遍历 AlterExpression
                //                for (AlterExpression alterExpression :
                // alterStatement.getAlterExpressions()) {
                //                    // 打印操作类型
                //                    System.out.println("Operation: " +
                // alterExpression.getOperation());
                //
                //                    if (alterExpression.getOperation() == AlterOperation.ADD) {
                //                        for (Column column : alterExpression.getColDataTypeList())
                // {
                //                            // 打印列信息
                //                            System.out.println("Column: " +
                // column.getColumnName());
                //                            System.out.println("Type: " +
                // column.getColDataType().getDataType());
                //                            System.out.println("Character Set: " +
                // alterExpression.getColumnSpecs().get(1));
                //                            System.out.println("Collate: " +
                // alterExpression.getColumnSpecs().get(3));
                //                            System.out.println("Default: " +
                // alterExpression.getColumnSpecs().get(5));
                //                            System.out.println("Comment: " +
                // alterExpression.getColumnSpecs().get(7));
                //                        }
                //                    }
                //                }
            } else {
                System.out.println("Not an ALTER TABLE statement");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
