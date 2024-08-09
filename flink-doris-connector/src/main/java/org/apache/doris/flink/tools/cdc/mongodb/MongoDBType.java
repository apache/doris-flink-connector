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

package org.apache.doris.flink.tools.cdc.mongodb;

import org.apache.flink.api.java.tuple.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.doris.flink.catalog.doris.DorisType;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.bson.BsonArray;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MongoDBType {

    public static final String DATE_TYPE = "$date";
    public static final String DECIMAL_TYPE = "$numberDecimal";
    public static final String LONG_TYPE = "$numberLong";

    public static String toDorisType(Object value) {
        if (value instanceof Integer) {
            return DorisType.INT;
        } else if (value instanceof Date) {
            return DorisType.DATETIME_V2 + "(3)";
        } else if (value instanceof Long) {
            return DorisType.BIGINT;
        } else if (value instanceof Double) {
            return checkAndRebuildBigDecimal(new BigDecimal(String.valueOf(value)));
        } else if (value instanceof Boolean) {
            return DorisType.BOOLEAN;
        } else if (value instanceof String) {
            return DorisType.STRING;
        } else if (value instanceof ObjectId) {
            return DorisType.VARCHAR + "(30)";
        } else if (value instanceof BsonArray) {
            return DorisType.ARRAY;
        } else if (value instanceof Decimal128) {
            return checkAndRebuildBigDecimal(((Decimal128) value).bigDecimalValue());
        } else {
            return DorisType.STRING;
        }
    }

    public static String jsonNodeToDorisType(JsonNode value) {
        if (value instanceof IntNode) {
            return DorisType.INT;
        } else if (value instanceof TextNode) {
            return DorisType.STRING;
        } else if (value instanceof LongNode) {
            return DorisType.BIGINT;
        } else if (value instanceof DoubleNode) {
            return DorisType.DOUBLE;
        } else if (value instanceof BooleanNode) {
            return DorisType.BOOLEAN;
        } else if (value instanceof ArrayNode) {
            return DorisType.ARRAY;
        } else if (value instanceof DecimalNode) {
            return checkAndRebuildBigDecimal(value.decimalValue());
        } else if (value instanceof ObjectNode) {
            if (value.size() == 1 && value.get(DATE_TYPE) != null) {
                return DorisType.DATETIME_V2 + "(3)";
            } else if (value.size() == 1 && value.get(DECIMAL_TYPE) != null) {
                return checkAndRebuildBigDecimal(new BigDecimal(value.get(DECIMAL_TYPE).asText()));
            } else if (value.size() == 1 && value.get(LONG_TYPE) != null) {
                return DorisType.BIGINT;
            } else {
                return DorisType.STRING;
            }
        } else {
            return DorisType.STRING;
        }
    }

    public static Tuple2<Integer, Integer> getDecimalPrecisionAndScale(String decimalString) {
        // Simplified regular expression to match two numbers in brackets
        String regex = "\\((\\d+),(\\d+)\\)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(decimalString);

        if (matcher.find()) {
            Integer precision = Integer.parseInt(matcher.group(1));
            Integer scale = Integer.parseInt(matcher.group(2));
            return new Tuple2<>(precision, scale);
        }
        throw new DorisRuntimeException("Get Decimal precision and Scale error !");
    }

    public static String checkAndRebuildBigDecimal(BigDecimal decimal) {
        if (decimal.scale() < 0) {
            decimal = new BigDecimal(decimal.toPlainString());
        }
        return decimal.precision() <= 38
                ? formatDecimalType(decimal.precision(), Math.max(decimal.scale(), 0))
                : DorisType.STRING;
    }

    public static String formatDecimalType(int precision, int scale) {
        return String.format("%s(%s,%s)", DorisType.DECIMAL_V3, precision, scale);
    }
}
