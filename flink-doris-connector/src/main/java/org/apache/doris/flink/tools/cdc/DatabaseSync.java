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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.apache.doris.flink.catalog.doris.DorisSystem;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisConnectionOptions;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.JsonDebeziumSchemaSerializer;
import org.apache.doris.flink.table.DorisConfigOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSync.class);
    private static final String LIGHT_SCHEMA_CHANGE = "light_schema_change";
    private static final String TABLE_NAME_OPTIONS = "table-name";

    protected Configuration config;

    protected String database;

    protected TableNameConverter converter;
    protected Pattern includingPattern;
    protected Pattern excludingPattern;
    protected Map<Pattern, String> multiToOneRulesPattern;
    protected Map<String, String> tableConfig = new HashMap<>();
    protected Configuration sinkConfig;
    protected boolean ignoreDefaultValue;

    public StreamExecutionEnvironment env;
    private boolean createTableOnly = false;
    private boolean newSchemaChange;
    protected String includingTables;
    protected String excludingTables;
    protected String multiToOneOrigin;
    protected String multiToOneTarget;
    protected String tablePrefix;
    protected String tableSuffix;
    protected boolean singleSink;
    private final Map<String, String> tableMapping = new HashMap<>();

    public abstract void registerDriver() throws SQLException;

    public abstract Connection getConnection() throws SQLException;

    public abstract List<SourceSchema> getSchemaList() throws Exception;

    public abstract DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env);

    /** Get the prefix of a specific tableList, for example, mysql is database, oracle is schema. */
    public abstract String getTableListPrefix();

    protected DatabaseSync() throws SQLException {
        registerDriver();
    }

    public void create() {
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.multiToOneRulesPattern = multiToOneRulesParser(multiToOneOrigin, multiToOneTarget);
        this.converter = new TableNameConverter(tablePrefix, tableSuffix, multiToOneRulesPattern);
        // default enable light schema change
        if (!this.tableConfig.containsKey(LIGHT_SCHEMA_CHANGE)) {
            this.tableConfig.put(LIGHT_SCHEMA_CHANGE, "true");
        }
    }

    public void build() throws Exception {
        DorisConnectionOptions options = getDorisConnectionOptions();
        DorisSystem dorisSystem = new DorisSystem(options);

        List<SourceSchema> schemaList = getSchemaList();
        Preconditions.checkState(!schemaList.isEmpty(), "No tables to be synchronized.");
        if (!dorisSystem.databaseExists(database)) {
            LOG.info("database {} not exist, created", database);
            dorisSystem.createDatabase(database);
        }

        List<String> syncTables = new ArrayList<>();
        List<String> dorisTables = new ArrayList<>();

        Map<String, Integer> tableBucketsMap = null;
        if (tableConfig.containsKey("table-buckets")) {
            tableBucketsMap = getTableBuckets(tableConfig.get("table-buckets"));
        }
        Set<String> bucketsTable = new HashSet<>();
        for (SourceSchema schema : schemaList) {
            syncTables.add(schema.getTableName());
            String dorisTable = converter.convert(schema.getTableName());

            // Calculate the mapping relationship between upstream and downstream tables
            tableMapping.put(
                    schema.getTableIdentifier(), String.format("%s.%s", database, dorisTable));
            if (!dorisSystem.tableExists(database, dorisTable)) {
                TableSchema dorisSchema = schema.convertTableSchema(tableConfig);
                // set doris target database
                dorisSchema.setDatabase(database);
                dorisSchema.setTable(dorisTable);
                if (tableBucketsMap != null) {
                    setTableSchemaBuckets(tableBucketsMap, dorisSchema, dorisTable, bucketsTable);
                }
                dorisSystem.createTable(dorisSchema);
            }
            if (!dorisTables.contains(dorisTable)) {
                dorisTables.add(dorisTable);
            }
        }
        if (createTableOnly) {
            System.out.println("Create table finished.");
            System.exit(0);
        }
        config.setString(TABLE_NAME_OPTIONS, getSyncTableList(syncTables));
        DataStreamSource<String> streamSource = buildCdcSource(env);
        if (singleSink) {
            streamSource.sinkTo(buildDorisSink());
        } else {
            SingleOutputStreamOperator<Void> parsedStream =
                    streamSource.process(new ParsingProcessFunction(converter));
            for (String table : dorisTables) {
                OutputTag<String> recordOutputTag =
                        ParsingProcessFunction.createRecordOutputTag(table);
                DataStream<String> sideOutput = parsedStream.getSideOutput(recordOutputTag);
                int sinkParallel =
                        sinkConfig.getInteger(
                                DorisConfigOptions.SINK_PARALLELISM, sideOutput.getParallelism());
                sideOutput
                        .sinkTo(buildDorisSink(table))
                        .setParallelism(sinkParallel)
                        .name(table)
                        .uid(table);
            }
        }
    }

    private DorisConnectionOptions getDorisConnectionOptions() {
        String fenodes = sinkConfig.getString(DorisConfigOptions.FENODES);
        String benodes = sinkConfig.getString(DorisConfigOptions.BENODES);
        String user = sinkConfig.getString(DorisConfigOptions.USERNAME);
        String passwd = sinkConfig.getString(DorisConfigOptions.PASSWORD, "");
        String jdbcUrl = sinkConfig.getString(DorisConfigOptions.JDBC_URL);
        Preconditions.checkNotNull(fenodes, "fenodes is empty in sink-conf");
        Preconditions.checkNotNull(user, "username is empty in sink-conf");
        Preconditions.checkNotNull(jdbcUrl, "jdbcurl is empty in sink-conf");
        DorisConnectionOptions.DorisConnectionOptionsBuilder builder =
                new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                        .withFenodes(fenodes)
                        .withBenodes(benodes)
                        .withUsername(user)
                        .withPassword(passwd)
                        .withJdbcUrl(jdbcUrl);
        return builder.build();
    }

    /** create doris sink for multi table. */
    public DorisSink<String> buildDorisSink() {
        return buildDorisSink(null);
    }

    /** create doris sink. */
    public DorisSink<String> buildDorisSink(String table) {
        String fenodes = sinkConfig.getString(DorisConfigOptions.FENODES);
        String benodes = sinkConfig.getString(DorisConfigOptions.BENODES);
        String user = sinkConfig.getString(DorisConfigOptions.USERNAME);
        String passwd = sinkConfig.getString(DorisConfigOptions.PASSWORD, "");

        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes(fenodes).setBenodes(benodes).setUsername(user).setPassword(passwd);
        sinkConfig
                .getOptional(DorisConfigOptions.AUTO_REDIRECT)
                .ifPresent(dorisBuilder::setAutoRedirect);

        // single sink not need table identifier
        if (!singleSink && !StringUtils.isNullOrWhitespaceOnly(table)) {
            dorisBuilder.setTableIdentifier(database + "." + table);
        }

        Properties pro = new Properties();
        // default json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        // customer stream load properties
        Properties streamLoadProp = DorisConfigOptions.getStreamLoadProp(sinkConfig.toMap());
        pro.putAll(streamLoadProp);
        DorisExecutionOptions.Builder executionBuilder =
                DorisExecutionOptions.builder().setStreamLoadProp(pro);

        sinkConfig
                .getOptional(DorisConfigOptions.SINK_LABEL_PREFIX)
                .ifPresent(executionBuilder::setLabelPrefix);
        sinkConfig
                .getOptional(DorisConfigOptions.SINK_ENABLE_DELETE)
                .ifPresent(executionBuilder::setDeletable);
        sinkConfig
                .getOptional(DorisConfigOptions.SINK_BUFFER_COUNT)
                .ifPresent(executionBuilder::setBufferCount);
        sinkConfig
                .getOptional(DorisConfigOptions.SINK_BUFFER_SIZE)
                .ifPresent(executionBuilder::setBufferSize);
        sinkConfig
                .getOptional(DorisConfigOptions.SINK_CHECK_INTERVAL)
                .ifPresent(executionBuilder::setCheckInterval);
        sinkConfig
                .getOptional(DorisConfigOptions.SINK_MAX_RETRIES)
                .ifPresent(executionBuilder::setMaxRetries);
        sinkConfig
                .getOptional(DorisConfigOptions.SINK_IGNORE_UPDATE_BEFORE)
                .ifPresent(executionBuilder::setIgnoreUpdateBefore);

        if (!sinkConfig.getBoolean(DorisConfigOptions.SINK_ENABLE_2PC)) {
            executionBuilder.disable2PC();
        } else if (sinkConfig.getOptional(DorisConfigOptions.SINK_ENABLE_2PC).isPresent()) {
            // force open 2pc
            executionBuilder.enable2PC();
        }

        sinkConfig
                .getOptional(DorisConfigOptions.SINK_ENABLE_BATCH_MODE)
                .ifPresent(executionBuilder::setBatchMode);
        sinkConfig
                .getOptional(DorisConfigOptions.SINK_FLUSH_QUEUE_SIZE)
                .ifPresent(executionBuilder::setFlushQueueSize);
        sinkConfig
                .getOptional(DorisConfigOptions.SINK_BUFFER_FLUSH_MAX_ROWS)
                .ifPresent(executionBuilder::setBufferFlushMaxRows);
        sinkConfig
                .getOptional(DorisConfigOptions.SINK_BUFFER_FLUSH_MAX_BYTES)
                .ifPresent(executionBuilder::setBufferFlushMaxBytes);
        sinkConfig
                .getOptional(DorisConfigOptions.SINK_BUFFER_FLUSH_INTERVAL)
                .ifPresent(v -> executionBuilder.setBufferFlushIntervalMs(v.toMillis()));

        sinkConfig
                .getOptional(DorisConfigOptions.SINK_USE_CACHE)
                .ifPresent(executionBuilder::setUseCache);

        DorisExecutionOptions executionOptions = executionBuilder.build();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(
                        JsonDebeziumSchemaSerializer.builder()
                                .setDorisOptions(dorisBuilder.build())
                                .setNewSchemaChange(newSchemaChange)
                                .setExecutionOptions(executionOptions)
                                .setTableMapping(tableMapping)
                                .setTableProperties(tableConfig)
                                .setTargetDatabase(database)
                                .setTargetTablePrefix(tablePrefix)
                                .setTargetTableSuffix(tableSuffix)
                                .build())
                .setDorisOptions(dorisBuilder.build());
        return builder.build();
    }

    /** Filter table that need to be synchronized. */
    protected boolean isSyncNeeded(String tableName) {
        boolean sync = true;
        if (includingPattern != null) {
            sync = includingPattern.matcher(tableName).matches();
        }
        if (excludingPattern != null) {
            sync = sync && !excludingPattern.matcher(tableName).matches();
        }
        LOG.debug("table {} is synchronized? {}", tableName, sync);
        return sync;
    }

    protected String getSyncTableList(List<String> syncTables) {
        if (!singleSink) {
            return syncTables.stream()
                    .map(v -> getTableListPrefix() + "\\." + v)
                    .collect(Collectors.joining("|"));
        } else {
            // includingTablePattern and ^excludingPattern
            if (includingTables == null) {
                includingTables = ".*";
            }
            String includingPattern =
                    String.format("(%s)\\.(%s)", getTableListPrefix(), includingTables);
            if (StringUtils.isNullOrWhitespaceOnly(excludingTables)) {
                return includingPattern;
            } else {
                String excludingPattern =
                        String.format("?!(%s\\.(%s))$", getTableListPrefix(), excludingTables);
                return String.format("(%s)(%s)", includingPattern, excludingPattern);
            }
        }
    }

    /** Filter table that many tables merge to one. */
    protected HashMap<Pattern, String> multiToOneRulesParser(
            String multiToOneOrigin, String multiToOneTarget) {
        if (StringUtils.isNullOrWhitespaceOnly(multiToOneOrigin)
                || StringUtils.isNullOrWhitespaceOnly(multiToOneTarget)) {
            return null;
        }
        HashMap<Pattern, String> multiToOneRulesPattern = new HashMap<>();
        String[] origins = multiToOneOrigin.split("\\|");
        String[] targets = multiToOneTarget.split("\\|");
        if (origins.length != targets.length) {
            System.out.println(
                    "param error : multi to one params length are not equal,please check your params.");
            System.exit(1);
        }
        try {
            for (int i = 0; i < origins.length; i++) {
                multiToOneRulesPattern.put(Pattern.compile(origins[i]), targets[i]);
            }
        } catch (Exception e) {
            System.out.println("param error : Your regular expression is incorrect,please check.");
            System.exit(1);
        }
        return multiToOneRulesPattern;
    }

    /**
     * Get table buckets Map.
     *
     * @param tableBuckets the string of tableBuckets, eg:student:10,student_info:20,student.*:30
     * @return The table name and buckets map. The key is table name, the value is buckets.
     */
    public static Map<String, Integer> getTableBuckets(String tableBuckets) {
        Map<String, Integer> tableBucketsMap = new LinkedHashMap<>();
        String[] tableBucketsArray = tableBuckets.split(",");
        for (String tableBucket : tableBucketsArray) {
            String[] tableBucketArray = tableBucket.split(":");
            tableBucketsMap.put(
                    tableBucketArray[0].trim(), Integer.parseInt(tableBucketArray[1].trim()));
        }
        return tableBucketsMap;
    }

    /**
     * Set table schema buckets.
     *
     * @param tableBucketsMap The table name and buckets map. The key is table name, the value is
     *     buckets.
     * @param dorisSchema @{TableSchema}
     * @param dorisTable the table name need to set buckets
     * @param tableHasSet The buckets table is set
     */
    public void setTableSchemaBuckets(
            Map<String, Integer> tableBucketsMap,
            TableSchema dorisSchema,
            String dorisTable,
            Set<String> tableHasSet) {

        if (tableBucketsMap != null) {
            // Firstly, if the table name is in the table-buckets map, set the buckets of the table.
            if (tableBucketsMap.containsKey(dorisTable)) {
                dorisSchema.setTableBuckets(tableBucketsMap.get(dorisTable));
                tableHasSet.add(dorisTable);
                return;
            }
            // Secondly, iterate over the map to find a corresponding regular expression match,
            for (Map.Entry<String, Integer> entry : tableBucketsMap.entrySet()) {
                if (tableHasSet.contains(entry.getKey())) {
                    continue;
                }

                Pattern pattern = Pattern.compile(entry.getKey());
                if (pattern.matcher(dorisTable).matches()) {
                    dorisSchema.setTableBuckets(entry.getValue());
                    tableHasSet.add(dorisTable);
                    return;
                }
            }
        }
    }

    public DatabaseSync setEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public DatabaseSync setConfig(Configuration config) {
        this.config = config;
        return this;
    }

    public DatabaseSync setDatabase(String database) {
        this.database = database;
        return this;
    }

    public DatabaseSync setIncludingTables(String includingTables) {
        this.includingTables = includingTables;
        return this;
    }

    public DatabaseSync setExcludingTables(String excludingTables) {
        this.excludingTables = excludingTables;
        return this;
    }

    public DatabaseSync setMultiToOneOrigin(String multiToOneOrigin) {
        this.multiToOneOrigin = multiToOneOrigin;
        return this;
    }

    public DatabaseSync setMultiToOneTarget(String multiToOneTarget) {
        this.multiToOneTarget = multiToOneTarget;
        return this;
    }

    public DatabaseSync setTableConfig(Map<String, String> tableConfig) {
        if (!CollectionUtil.isNullOrEmpty(tableConfig)) {
            this.tableConfig = tableConfig;
        }
        return this;
    }

    public DatabaseSync setSinkConfig(Configuration sinkConfig) {
        this.sinkConfig = sinkConfig;
        return this;
    }

    public DatabaseSync setIgnoreDefaultValue(boolean ignoreDefaultValue) {
        this.ignoreDefaultValue = ignoreDefaultValue;
        return this;
    }

    public DatabaseSync setCreateTableOnly(boolean createTableOnly) {
        this.createTableOnly = createTableOnly;
        return this;
    }

    public DatabaseSync setNewSchemaChange(boolean newSchemaChange) {
        this.newSchemaChange = newSchemaChange;
        return this;
    }

    public DatabaseSync setSingleSink(boolean singleSink) {
        this.singleSink = singleSink;
        return this;
    }

    public DatabaseSync setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    public DatabaseSync setTableSuffix(String tableSuffix) {
        this.tableSuffix = tableSuffix;
        return this;
    }

    public static class TableNameConverter implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String prefix;
        private final String suffix;
        private Map<Pattern, String> multiToOneRulesPattern;

        TableNameConverter() {
            this("", "");
        }

        TableNameConverter(String prefix, String suffix) {
            this.prefix = prefix == null ? "" : prefix;
            this.suffix = suffix == null ? "" : suffix;
        }

        TableNameConverter(
                String prefix, String suffix, Map<Pattern, String> multiToOneRulesPattern) {
            this.prefix = prefix == null ? "" : prefix;
            this.suffix = suffix == null ? "" : suffix;
            this.multiToOneRulesPattern = multiToOneRulesPattern;
        }

        public String convert(String tableName) {
            if (multiToOneRulesPattern == null) {
                return prefix + tableName + suffix;
            }

            String target = null;

            for (Map.Entry<Pattern, String> patternStringEntry :
                    multiToOneRulesPattern.entrySet()) {
                if (patternStringEntry.getKey().matcher(tableName).matches()) {
                    target = patternStringEntry.getValue();
                }
            }
            /**
             * If multiToOneRulesPattern is not null and target is not assigned, then the
             * synchronization task contains both multi to one and one to one , prefixes and
             * suffixes are added to common one-to-one mapping tables
             */
            if (target == null) {
                return prefix + tableName + suffix;
            }
            return target;
        }
    }
}
