// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.sink.schema;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterExpression.ColumnDataType;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.flink.catalog.doris.DorisSchemaFactory;
import org.apache.doris.flink.catalog.doris.DorisType;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.sink.writer.serializer.jsondebezium.JsonDebeziumChangeUtils;
import org.apache.doris.flink.tools.cdc.DorisTableConfig;
import org.apache.doris.flink.tools.cdc.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Use {@link CCJSqlParserUtil} to parse SQL statements. */
public class SQLParserSchemaManager implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SQLParserSchemaManager.class);
    private static final String DEFAULT = "DEFAULT";
    private static final String COMMENT = "COMMENT";
    private static final String PRIMARY = "PRIMARY";
    private static final String PRIMARY_KEY = "PRIMARY KEY";
    private static final String UNIQUE = "UNIQUE";
    private static final String DORIS_CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";
    private static final Set<String> sourceConnectorTimeValues =
            new HashSet<>(
                    Arrays.asList(
                            "SYSDATE",
                            "SYSTIMESTAMP",
                            "CURRENT_TIMESTAMP",
                            "NOW()",
                            "CURRENT TIMESTAMP",
                            "GETDATE()"));

    /**
     * Doris' schema change only supports ADD, DROP, and RENAME operations. This method is only used
     * to parse the above schema change operations.
     */
    public List<String> parseAlterDDLs(
            SourceConnector sourceConnector, String ddl, String dorisTable) {
        List<String> ddlList = new ArrayList<>();
        try {
            Statement statement = CCJSqlParserUtil.parse(ddl);
            if (statement instanceof Alter) {
                Alter alterStatement = (Alter) statement;
                List<AlterExpression> alterExpressions = alterStatement.getAlterExpressions();
                for (AlterExpression alterExpression : alterExpressions) {
                    AlterOperation operation = alterExpression.getOperation();
                    switch (operation) {
                        case DROP:
                            String dropColumnDDL =
                                    processDropColumnOperation(alterExpression, dorisTable);
                            ddlList.add(dropColumnDDL);
                            break;
                        case ADD:
                            List<String> addColumnDDL =
                                    processAddColumnOperation(
                                            sourceConnector, alterExpression, dorisTable);
                            ddlList.addAll(addColumnDDL);
                            break;
                        case CHANGE:
                            String changeColumnDDL =
                                    processChangeColumnOperation(alterExpression, dorisTable);
                            ddlList.add(changeColumnDDL);
                            break;
                        case RENAME:
                            String renameColumnDDL =
                                    processRenameColumnOperation(alterExpression, dorisTable);
                            ddlList.add(renameColumnDDL);
                            break;
                        default:
                            LOG.warn(
                                    "Unsupported alter ddl operations, operation={}, ddl={}",
                                    operation.name(),
                                    ddl);
                    }
                }
            } else {
                LOG.warn("Unsupported ddl operations, ddl={}", ddl);
            }
        } catch (JSQLParserException e) {
            LOG.warn("Failed to parse DDL SQL, SQL={}", ddl, e);
        }
        return ddlList;
    }

    public TableSchema parseCreateTableStatement(
            SourceConnector sourceConnector,
            String ddl,
            String dorisTable,
            DorisTableConfig dorisTableConfig) {
        try {
            Statement statement = CCJSqlParserUtil.parse(ddl);
            if (statement instanceof CreateTable) {
                CreateTable createTable = (CreateTable) statement;
                Map<String, FieldSchema> columnFields = new LinkedHashMap<>();
                List<String> pkKeys = new ArrayList<>();
                createTable
                        .getColumnDefinitions()
                        .forEach(
                                column -> {
                                    String columnName = column.getColumnName();
                                    ColDataType colDataType = column.getColDataType();
                                    String dataType = parseDataType(colDataType, sourceConnector);
                                    List<String> columnSpecs = column.getColumnSpecs();
                                    String defaultValue =
                                            extractDefaultValue(dataType, columnSpecs);
                                    String comment = extractComment(columnSpecs);
                                    FieldSchema fieldSchema =
                                            new FieldSchema(
                                                    columnName, dataType, defaultValue, comment);
                                    columnFields.put(columnName, fieldSchema);
                                    extractColumnPrimaryKey(columnName, columnSpecs, pkKeys);
                                });

                List<Index> indexes = createTable.getIndexes();
                extractIndexesPrimaryKey(indexes, pkKeys);
                String[] dbTable = dorisTable.split("\\.");
                Preconditions.checkArgument(dbTable.length == 2);

                return DorisSchemaFactory.createTableSchema(
                        dbTable[0],
                        dbTable[1],
                        columnFields,
                        pkKeys,
                        dorisTableConfig,
                        extractTableComment(createTable.getTableOptionsStrings()));
            } else {
                LOG.warn(
                        "Unsupported statement type. ddl={}, sourceConnector={}, dorisTable={}",
                        ddl,
                        sourceConnector.getConnectorName(),
                        dorisTable);
            }
        } catch (JSQLParserException e) {
            LOG.warn(
                    "Failed to parse create table statement. ddl={}, sourceConnector={}, dorisTable={}",
                    ddl,
                    sourceConnector.getConnectorName(),
                    dorisTable);
        }
        return null;
    }

    private void extractIndexesPrimaryKey(List<Index> indexes, List<String> pkKeys) {
        if (CollectionUtils.isEmpty(indexes)) {
            return;
        }
        indexes.stream()
                .filter(
                        index ->
                                PRIMARY_KEY.equalsIgnoreCase(index.getType())
                                        || UNIQUE.equalsIgnoreCase(index.getType()))
                .flatMap(index -> index.getColumnsNames().stream())
                .distinct()
                .filter(
                        primaryKey ->
                                pkKeys.stream()
                                        .noneMatch(pkKey -> pkKey.equalsIgnoreCase(primaryKey)))
                .forEach(pkKeys::add);
    }

    private void extractColumnPrimaryKey(
            String columnName, List<String> columnSpecs, List<String> pkKeys) {
        if (CollectionUtils.isEmpty(columnSpecs)) {
            return;
        }
        for (String columnSpec : columnSpecs) {
            if (PRIMARY.equalsIgnoreCase(columnSpec)) {
                pkKeys.add(columnName);
            }
        }
    }

    private String extractTableComment(List<String> tableOptionsStrings) {
        if (CollectionUtils.isEmpty(tableOptionsStrings)) {
            return null;
        }
        return extractAdjacentString(tableOptionsStrings, COMMENT);
    }

    private String parseDataType(ColDataType colDataType, SourceConnector sourceConnector) {
        String dataType = colDataType.getDataType();
        int length = 0;
        int scale = 0;
        if (CollectionUtils.isNotEmpty(colDataType.getArgumentsStringList())) {
            List<String> argumentsStringList = colDataType.getArgumentsStringList();
            length = Integer.parseInt(argumentsStringList.get(0));
            if (argumentsStringList.size() == 2) {
                scale = Integer.parseInt(argumentsStringList.get(1));
            }
        }
        return JsonDebeziumChangeUtils.buildDorisTypeName(sourceConnector, dataType, length, scale);
    }

    private String processDropColumnOperation(AlterExpression alterExpression, String dorisTable) {
        String dropColumnDDL =
                SchemaChangeHelper.buildDropColumnDDL(dorisTable, alterExpression.getColumnName());
        LOG.info("Parsed drop column DDL SQL is: {}", dropColumnDDL);
        return dropColumnDDL;
    }

    private List<String> processAddColumnOperation(
            SourceConnector sourceConnector, AlterExpression alterExpression, String dorisTable) {
        List<ColumnDataType> colDataTypeList = alterExpression.getColDataTypeList();
        List<String> addColumnList = new ArrayList<>();
        for (ColumnDataType columnDataType : colDataTypeList) {
            String columnName = columnDataType.getColumnName();
            ColDataType colDataType = columnDataType.getColDataType();
            String datatype = parseDataType(colDataType, sourceConnector);

            List<String> columnSpecs = columnDataType.getColumnSpecs();
            String defaultValue = extractDefaultValue(datatype, columnSpecs);
            String comment = extractComment(columnSpecs);
            FieldSchema fieldSchema = new FieldSchema(columnName, datatype, defaultValue, comment);
            String addColumnDDL = SchemaChangeHelper.buildAddColumnDDL(dorisTable, fieldSchema);
            LOG.info("Parsed add column DDL SQL is: {}", addColumnDDL);
            addColumnList.add(addColumnDDL);
        }
        return addColumnList;
    }

    private String processChangeColumnOperation(
            AlterExpression alterExpression, String dorisTable) {
        String columnNewName = alterExpression.getColDataTypeList().get(0).getColumnName();
        String columnOldName = alterExpression.getColumnOldName();
        String renameColumnDDL =
                SchemaChangeHelper.buildRenameColumnDDL(dorisTable, columnOldName, columnNewName);
        LOG.warn(
                "Note: Only rename column names are supported in doris. "
                        + "Therefore, the change syntax used here only supports the use of rename."
                        + " Parsed change column DDL SQL is: {}",
                renameColumnDDL);
        return renameColumnDDL;
    }

    private String processRenameColumnOperation(
            AlterExpression alterExpression, String dorisTable) {
        String columnNewName = alterExpression.getColumnName();
        String columnOldName = alterExpression.getColumnOldName();
        String renameColumnDDL =
                SchemaChangeHelper.buildRenameColumnDDL(dorisTable, columnOldName, columnNewName);
        LOG.info("Parsed rename column DDL SQL is: {}", renameColumnDDL);
        return renameColumnDDL;
    }

    @VisibleForTesting
    public String extractDefaultValue(String dateType, List<String> columnSpecs) {
        if (CollectionUtils.isEmpty(columnSpecs)) {
            return null;
        }
        String adjacentDefaultValue = extractAdjacentString(columnSpecs, DEFAULT);
        return parseDorisDefaultValue(dateType, adjacentDefaultValue);
    }

    private String parseDorisDefaultValue(String dateType, String defaultValue) {
        if (Objects.isNull(defaultValue)) {
            return null;
        }
        // In doris, DATETIME supports specifying the current time by default through
        // CURRENT_TIMESTAMP.
        if ((dateType.startsWith(DorisType.DATETIME) || dateType.startsWith(DorisType.DATETIME_V2))
                && sourceConnectorTimeValues.contains(defaultValue.toUpperCase(Locale.ROOT))) {
            return DORIS_CURRENT_TIMESTAMP;
        }
        return defaultValue;
    }

    private String extractAdjacentString(List<String> columnSpecs, String key) {
        int columnSpecsSize = columnSpecs.size();
        for (int i = 0; i < columnSpecsSize; i++) {
            String columnSpec = columnSpecs.get(i);
            if (key.equalsIgnoreCase(columnSpec) && i < columnSpecsSize - 1) {
                String adjacentString = columnSpecs.get(i + 1);
                if (!(DEFAULT.equalsIgnoreCase(adjacentString))
                        && !(COMMENT.equalsIgnoreCase(adjacentString))) {
                    return removeQuotes(adjacentString);
                }
                LOG.warn(
                        "Failed to extract adjacent string value. columnSpecs={}, key={}",
                        String.join(",", columnSpecs),
                        key);
            }
        }
        return null;
    }

    @VisibleForTesting
    public String extractComment(List<String> columnSpecs) {
        if (CollectionUtils.isEmpty(columnSpecs)) {
            return null;
        }
        return extractAdjacentString(columnSpecs, COMMENT);
    }

    private String removeQuotes(String content) {
        content = removeContinuousChar(content, '\'');
        content = removeContinuousChar(content, '\"');
        return content;
    }

    /**
     * remove the continuous char in the string from both sides.
     *
     * @param str the input string, target the char to be removed
     * @return the string without continuous chars from both sides
     */
    @VisibleForTesting
    public String removeContinuousChar(String str, char target) {
        if (str == null || str.length() < 2) {
            return str;
        }
        int start = 0;
        int end = str.length() - 1;
        while (start <= end && str.charAt(start) == target) {
            start++;
        }
        while (end >= start && str.charAt(end) == target) {
            end--;
        }
        return str.substring(start, end + 1);
    }
}
