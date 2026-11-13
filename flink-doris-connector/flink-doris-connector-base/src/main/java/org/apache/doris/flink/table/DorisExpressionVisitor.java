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

package org.apache.doris.flink.table;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.StringUtils;

import org.apache.doris.flink.exception.DorisRuntimeException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class DorisExpressionVisitor implements ExpressionVisitor<String> {
    private static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String DATETIMEV2_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DATETIME_PATTERN);
    DateTimeFormatter dateTimev2Formatter = DateTimeFormatter.ofPattern(DATETIMEV2_PATTERN);

    @Override
    public String visit(CallExpression call) {
        if (BuiltInFunctionDefinitions.EQUALS.equals(call.getFunctionDefinition())) {
            return combineExpression("=", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.LESS_THAN.equals(call.getFunctionDefinition())) {
            return combineExpression("<", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL.equals(call.getFunctionDefinition())) {
            return combineExpression("<=", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.GREATER_THAN.equals(call.getFunctionDefinition())) {
            return combineExpression(">", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL.equals(call.getFunctionDefinition())) {
            return combineExpression(">=", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.NOT_EQUALS.equals(call.getFunctionDefinition())) {
            return combineExpression("<>", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.OR.equals(call.getFunctionDefinition())) {
            return combineExpression("OR", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.AND.equals(call.getFunctionDefinition())) {
            return combineExpression("AND", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.LIKE.equals(call.getFunctionDefinition())) {
            return combineExpression("LIKE", call.getResolvedChildren());
        }
        if (BuiltInFunctionDefinitions.IS_NULL.equals(call.getFunctionDefinition())) {
            return combineLeftExpression("IS NULL", call.getResolvedChildren().get(0));
        }
        if (BuiltInFunctionDefinitions.IS_NOT_NULL.equals(call.getFunctionDefinition())) {
            return combineLeftExpression("IS NOT NULL", call.getResolvedChildren().get(0));
        }

        if (BuiltInFunctionDefinitions.CAST.equals(call.getFunctionDefinition())) {
            return call.getChildren().get(0).accept(this);
        }
        return null;
    }

    private String combineExpression(String operator, List<ResolvedExpression> operand) {
        String left = operand.get(0).accept(this);
        if (StringUtils.isNullOrWhitespaceOnly(left)) {
            return null;
        }
        String right = operand.get(1).accept(this);
        if (StringUtils.isNullOrWhitespaceOnly(right)) {
            return null;
        }
        return String.format("(%s %s %s)", left, operator, right);
    }

    private String combineLeftExpression(String operator, ResolvedExpression operand) {
        String left = operand.accept(this);
        return String.format("(%s %s)", left, operator);
    }

    @Override
    public String visit(ValueLiteralExpression valueLiteral) {
        LogicalTypeRoot typeRoot = valueLiteral.getOutputDataType().getLogicalType().getTypeRoot();

        switch (typeRoot) {
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case DATE:
                return "'" + valueLiteral + "'";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                Class<?> conversionClass = valueLiteral.getOutputDataType().getConversionClass();
                if (LocalDateTime.class.isAssignableFrom(conversionClass)) {
                    try {
                        LocalDateTime localDateTime =
                                valueLiteral
                                        .getValueAs(LocalDateTime.class)
                                        .orElseThrow(
                                                () ->
                                                        new RuntimeException(
                                                                "Failed to get LocalDateTime value"));
                        int nano = localDateTime.getNano();
                        if (nano == 0) {
                            // if nanoseconds equals to zero, the timestamp is in seconds.
                            return wrapWithQuotes(localDateTime.format(dateTimeFormatter));
                        } else {
                            // 1. Even though the datetime precision in Doris is set to 3, the
                            // microseconds format such as "yyyy-MM-dd HH:mm:ss.SSSSSS" can still
                            // function properly in the Doris query plan.
                            // 2. If the timestamp is in nanoseconds, format it like 'yyyy-MM-dd
                            // HH:mm:ss.SSSSSS'. This will have no impact on the result. Because
                            // when parsing the imported DATETIME type data on the BE side (for
                            // example, through Stream load, Spark load, etc.), or when using the FE
                            // side with Nereids enabled, the decimals that exceed the current
                            // precision will be rounded.
                            return wrapWithQuotes(localDateTime.format(dateTimev2Formatter));
                        }

                    } catch (Exception e) {
                        throw new DorisRuntimeException(e.getMessage());
                    }
                }
                break;
            default:
                return valueLiteral.toString();
        }
        return valueLiteral.toString();
    }

    @Override
    public String visit(FieldReferenceExpression fieldReference) {
        return fieldReference.getName();
    }

    @Override
    public String visit(TypeLiteralExpression typeLiteral) {
        return typeLiteral.getOutputDataType().toString();
    }

    @Override
    public String visit(Expression expression) {
        return null;
    }

    private static String wrapWithQuotes(String value) {
        return "'" + value + "'";
    }
}
