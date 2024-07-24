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

package org.apache.doris.flink.tools.cdc.oracle;

import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import oracle.sql.TIMESTAMP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleDateConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    private static final Logger log = LoggerFactory.getLogger(OracleDateConverter.class);
    private static final Pattern TO_DATE =
            Pattern.compile("TO_DATE\\('(.*)',[ ]*'(.*)'\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TO_TIMESTAMP =
            Pattern.compile("TO_TIMESTAMP\\('(.*)'\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TIMESTAMP_OR_DATE_REGEX =
            Pattern.compile("^TIMESTAMP[(]\\d[)]$|^DATE$", Pattern.CASE_INSENSITIVE);
    private ZoneId timestampZoneId = ZoneId.systemDefault();
    public static final Properties DEFAULT_PROPS = new Properties();
    private static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String DATETIMEV2_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private final DateTimeFormatter dateTimeV2Formatter =
            DateTimeFormatter.ofPattern(DATETIMEV2_PATTERN);

    static {
        DEFAULT_PROPS.setProperty("converters", "oracleDate");
        DEFAULT_PROPS.setProperty(
                "oracleDate.type", "org.apache.doris.flink.tools.cdc.oracle.OracleDateConverter");
    }

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .toFormatter();

    @Override
    public void configure(Properties props) {}

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String typeName = column.typeName();
        if (TIMESTAMP_OR_DATE_REGEX.matcher(typeName).matches()) {
            registration.register(
                    SchemaBuilder.string().optional(),
                    value -> {
                        if (value == null) {
                            if (column.isOptional()) {
                                return null;
                            } else if (column.hasDefaultValue()) {
                                return column.defaultValue();
                            } else {
                                return null;
                            }
                        }

                        if (value instanceof String) {
                            return convertStringTimestamp((String) value);
                        }
                        if (value instanceof Timestamp) {
                            return dateTimeV2Formatter.format(
                                    ((Timestamp) value).toLocalDateTime());
                        }

                        // oracle timestamp
                        try {
                            if (value instanceof TIMESTAMP) {
                                return dateTimeV2Formatter.format(
                                        ((TIMESTAMP) value).timestampValue().toLocalDateTime());
                            }
                        } catch (SQLException ex) {
                            log.error("convert timestamp failed, values is {}", value);
                        }

                        return null;
                    });
        }
    }

    private String convertStringTimestamp(String data) {
        LocalDateTime dateTime;

        final Matcher toTimestampMatcher = TO_TIMESTAMP.matcher(data);
        if (toTimestampMatcher.matches()) {
            String dateText = toTimestampMatcher.group(1);
            dateTime =
                    LocalDateTime.from(
                            TIMESTAMP_FORMATTER.parse(completeMilliseconds(dateText.trim())));
            return dateTimeV2Formatter.format(dateTime.atZone(timestampZoneId));
        }

        final Matcher toDateMatcher = TO_DATE.matcher(data);
        if (toDateMatcher.matches()) {
            String date = toDateMatcher.group(1);
            dateTime =
                    LocalDateTime.from(
                            TIMESTAMP_FORMATTER.parse(completeMilliseconds(date.trim())));
            return dateTimeV2Formatter.format(dateTime.atZone(timestampZoneId));
        }
        return null;
    }

    private String completeMilliseconds(String stringValue) {
        if (stringValue.length() == DATETIMEV2_PATTERN.length()) {
            return stringValue;
        }
        StringBuilder sb = new StringBuilder(stringValue);
        if (stringValue.length() == DATETIME_PATTERN.length()) {
            sb.append(".");
        }
        while (sb.toString().length() < DATETIMEV2_PATTERN.length()) {
            sb.append(0);
        }
        return sb.toString();
    }
}
