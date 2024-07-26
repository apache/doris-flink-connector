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

package org.apache.doris.flink.tools.cdc.db2;

import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.doris.flink.tools.cdc.DatabaseSyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.function.Consumer;

public class Db2DateConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Db2DateConverter.class);
    private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
    private DateTimeFormatter timestampFormatter = DateTimeFormatter.ISO_DATE_TIME;
    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;

    protected static final Properties DEFAULT_PROPS = new Properties();

    static {
        DEFAULT_PROPS.setProperty(DatabaseSyncConfig.CONVERTERS, DatabaseSyncConfig.DATE);
        DEFAULT_PROPS.setProperty(
                DatabaseSyncConfig.DATE_TYPE,
                "org.apache.doris.flink.tools.cdc.db2.Db2DateConverter");
        DEFAULT_PROPS.setProperty(
                DatabaseSyncConfig.DATE_FORMAT_DATE, DatabaseSyncConfig.YEAR_MONTH_DAY_FORMAT);
        DEFAULT_PROPS.setProperty(
                DatabaseSyncConfig.DATE_FORMAT_TIMESTAMP, DatabaseSyncConfig.DATETIME_MICRO_FORMAT);
    }

    @Override
    public void configure(Properties props) {
        readProps(props, "format.date", p -> dateFormatter = DateTimeFormatter.ofPattern(p));
        readProps(
                props,
                "format.timestamp",
                p -> timestampFormatter = DateTimeFormatter.ofPattern(p));
    }

    private void readProps(Properties properties, String settingKey, Consumer<String> callback) {
        String settingValue = (String) properties.get(settingKey);
        if (settingValue == null || settingValue.isEmpty()) {
            return;
        }
        try {
            callback.accept(settingValue.trim());
        } catch (IllegalArgumentException | DateTimeException e) {
            LOGGER.error("setting {} is illegal:{}", settingKey, settingValue);
            throw e;
        }
    }

    @Override
    public void converterFor(
            RelationalColumn column,
            CustomConverter.ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = column.typeName().toUpperCase();
        SchemaBuilder schemaBuilder = null;
        CustomConverter.Converter converter = null;
        if (DatabaseSyncConfig.UPPERCASE_DATE.equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional();
            converter = this::convertDate;
        }
        if (DatabaseSyncConfig.TIME.equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional();
            converter = this::convertTime;
        }
        if (DatabaseSyncConfig.TIMESTAMP.equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().optional();
            converter = this::convertTimestamp;
        }
        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
        }
    }

    private String convertDate(Object input) {
        if (input instanceof LocalDate) {
            return dateFormatter.format((LocalDate) input);
        } else if (input instanceof Integer) {
            LocalDate date = LocalDate.ofEpochDay((Integer) input);
            return dateFormatter.format(date);
        } else if (input instanceof Date) {
            return dateFormatter.format(((Date) input).toLocalDate());
        }
        return null;
    }

    private String convertTime(Object input) {
        if (input instanceof Time) {
            return timeFormatter.format(((Time) input).toLocalTime());
        }
        return null;
    }

    private String convertTimestamp(Object input) {
        if (input instanceof Timestamp) {
            return timestampFormatter.format(((Timestamp) input).toLocalDateTime());
        } else if (input instanceof Instant) {
            LocalDateTime ldt = LocalDateTime.ofInstant(((Instant) input), ZoneOffset.UTC);
            return timestampFormatter.format(ldt);
        }
        return null;
    }
}
