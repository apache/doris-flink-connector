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

package org.apache.doris.flink.tools.cdc;

public class DatabaseSyncConfig {

    public static final String MYSQL_SYNC_DATABASE = "mysql-sync-database";
    public static final String ORACLE_SYNC_DATABASE = "oracle-sync-database";
    public static final String POSTGRES_SYNC_DATABASE = "postgres-sync-database";
    public static final String SQLSERVER_SYNC_DATABASE = "sqlserver-sync-database";
    public static final String MONGODB_SYNC_DATABASE = "mongodb-sync-database";
    public static final String DB2_SYNC_DATABASE = "db2-sync-database";

    public static final String MYSQL_CONF = "mysql-conf";
    public static final String ORACLE_CONF = "oracle-conf";
    public static final String POSTGRES_CONF = "postgres-conf";
    public static final String SQLSERVER_CONF = "sqlserver-conf";
    public static final String MONGODB_CONF = "mongodb-conf";
    public static final String DB2_CONF = "db2-conf";

    ///////////// source-conf ////////
    public static final String DATABASE_NAME = "database-name";
    public static final String DB = "db";
    public static final String PORT = "port";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String TABLE_NAME = "TABLE_NAME";
    public static final String REMARKS = "REMARKS";

    ////////// cdc-conf //////////
    // config options of {@link
    // org.apache.flink.cdc.connectors.base.options.SourceOptions.SCAN_STARTUP_MODE}
    public static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    public static final String SCAN_STARTUP_MODE_VALUE_EARLIEST_OFFSET = "earliest-offset";
    public static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET = "specific-offset";
    public static final String SCAN_STARTUP_MODE_VALUE_LATEST_OFFSET = "latest-offset";
    public static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";
    public static final String DECIMAL_HANDLING_MODE = "decimal.handling.mode";

    ////////// sink-conf /////////////
    public static final String SINK_CONF = "sink-conf";
    public static final String JOB_NAME = "job-name";
    public static final String DATABASE = "database";
    public static final String TABLE_PREFIX = "table-prefix";
    public static final String TABLE_SUFFIX = "table-suffix";
    public static final String INCLUDING_TABLES = "including-tables";
    public static final String EXCLUDING_TABLES = "excluding-tables";
    public static final String MULTI_TO_ONE_ORIGIN = "multi-to-one-origin";
    public static final String MULTI_TO_ONE_TARGET = "multi-to-one-target";
    public static final String SCHEMA_CHANGE_MODE = "schema-change-mode";
    public static final String CREATE_TABLE_ONLY = "create-table-only";
    public static final String IGNORE_DEFAULT_VALUE = "ignore-default-value";
    public static final String IGNORE_INCOMPATIBLE = "ignore-incompatible";
    public static final String SINGLE_SINK = "single-sink";
    ////////// doris-table-conf //////////
    public static final String TABLE_CONF = "table-conf";
    public static final String REPLICATION_NUM = "replication_num";
    public static final String TABLE_BUCKETS = "table-buckets";

    ////////// date-converter-conf //////////
    public static final String CONVERTERS = "converters";
    public static final String DATE = "date";
    public static final String DATE_TYPE = "date.type";
    public static final String DATE_FORMAT_DATE = "date.format.date";
    public static final String DATE_FORMAT_DATETIME = "date.format.datetime";
    public static final String DATE_FORMAT_TIMESTAMP = "date.format.timestamp";
    public static final String DATE_FORMAT_TIMESTAMP_ZONE = "date.format.timestamp.zone";
    public static final String YEAR_MONTH_DAY_FORMAT = "yyyy-MM-dd";
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATETIME_MICRO_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    public static final String TIME_ZONE_SHANGHAI = "Asia/Shanghai";
    public static final String TIME_ZONE_UTC_8 = "UTC+8";
    public static final String FORMAT_DATE = "format.date";
    public static final String FORMAT_TIME = "format.time";
    public static final String FORMAT_DATETIME = "format.datetime";
    public static final String FORMAT_TIMESTAMP = "format.timestamp";
    public static final String FORMAT_TIMESTAMP_ZONE = "format.timestamp.zone";
    public static final String UPPERCASE_DATE = "DATE";
    public static final String TIME = "TIME";
    public static final String DATETIME = "DATETIME";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String SMALLDATETIME = "SMALLDATETIME";
    public static final String DATETIME2 = "DATETIME2";
}
