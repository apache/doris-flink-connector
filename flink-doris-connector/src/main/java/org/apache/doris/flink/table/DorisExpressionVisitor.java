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

import java.util.List;

public class DorisExpressionVisitor implements ExpressionVisitor<String> {

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
        return null;
    }

    private String combineExpression(String operator, List<ResolvedExpression> operand) {
        String left = operand.get(0).accept(this);
        String right = operand.get(1).accept(this);
        return String.format("(%s %s %s)", left, operator, right);
    }

    private String combineLeftExpression(String operator, ResolvedExpression operand) {
        String left = operand.accept(this);
        return String.format("(%s %s)", left, operator);
    }

    @Override
    public String visit(ValueLiteralExpression valueLiteral) {
        LogicalTypeRoot typeRoot = valueLiteral.getOutputDataType().getLogicalType().getTypeRoot();
        if (typeRoot.equals(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                || typeRoot.equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                || typeRoot.equals(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE)
                || typeRoot.equals(LogicalTypeRoot.DATE)) {
            return "'" + valueLiteral + "'";
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
}
