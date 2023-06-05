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

import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.doris.flink.catalog.doris.DorisSystem;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisConnectionOptions;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.JsonDebeziumSchemaSerializer;
import org.apache.doris.flink.table.DorisConfigOptions;
import org.apache.doris.flink.tools.cdc.mysql.ParsingProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public abstract class DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSync.class);
    private static final String LIGHT_SCHEMA_CHANGE = "light_schema_change";
    protected Configuration config;
    protected String database;
    protected TableNameConverter converter;
    protected Pattern includingPattern;
    protected Pattern excludingPattern;
    protected Map<String, String> tableConfig;
    protected Configuration sinkConfig;
    public StreamExecutionEnvironment env;

    public abstract Connection getConnection() throws SQLException;

    public abstract List<SourceSchema> getSchemaList() throws Exception;

    public abstract DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env);


    public void create(StreamExecutionEnvironment env, String database, Configuration config,
                       String tablePrefix, String tableSuffix, String includingTables,
                       String excludingTables, Configuration sinkConfig, Map<String, String> tableConfig) {
        this.env = env;
        this.config = config;
        this.database = database;
        this.converter = new TableNameConverter(tablePrefix, tableSuffix);
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.sinkConfig = sinkConfig;
        this.tableConfig = tableConfig == null ? new HashMap<>() : tableConfig;
        //default enable light schema change
        if(!this.tableConfig.containsKey(LIGHT_SCHEMA_CHANGE)){
            this.tableConfig.put(LIGHT_SCHEMA_CHANGE, "true");
        }
    }

    public void build() throws Exception {
        DorisConnectionOptions options = getDorisConnectionOptions();
        DorisSystem dorisSystem = new DorisSystem(options);

        List<SourceSchema> schemaList = getSchemaList();
        if (!dorisSystem.databaseExists(database)) {
            LOG.info("database {} not exist, created", database);
            dorisSystem.createDatabase(database);
        }

        List<String> syncTables = new ArrayList<>();
        List<String> dorisTables = new ArrayList<>();
        for (SourceSchema schema : schemaList) {
            syncTables.add(schema.getTableName());
            String dorisTable = converter.convert(schema.getTableName());
            if (!dorisSystem.tableExists(database, dorisTable)) {
                TableSchema dorisSchema = schema.convertTableSchema(tableConfig);
                //set doris target database
                dorisSchema.setDatabase(database);
                dorisSchema.setTable(dorisTable);
                dorisSystem.createTable(dorisSchema);
            }
            dorisTables.add(dorisTable);
        }
        Preconditions.checkState(!syncTables.isEmpty(), "No tables to be synchronized.");
        config.set(MySqlSourceOptions.TABLE_NAME, "(" + String.join("|", syncTables) + ")");

        DataStreamSource<String> streamSource = buildCdcSource(env);
        SingleOutputStreamOperator<Void> parsedStream = streamSource.process(new ParsingProcessFunction(converter));
        for (String table : dorisTables) {
            OutputTag<String> recordOutputTag = ParsingProcessFunction.createRecordOutputTag(table);
            DataStream<String> sideOutput = parsedStream.getSideOutput(recordOutputTag);

            int sinkParallel = sinkConfig.getInteger(DorisConfigOptions.SINK_PARALLELISM, sideOutput.getParallelism());
            sideOutput.sinkTo(buildDorisSink(table)).setParallelism(sinkParallel).name(table);
        }
    }

    private DorisConnectionOptions getDorisConnectionOptions() {
        String fenodes = sinkConfig.getString(DorisConfigOptions.FENODES);
        String user = sinkConfig.getString(DorisConfigOptions.USERNAME);
        String passwd = sinkConfig.getString(DorisConfigOptions.PASSWORD, "");
        String jdbcUrl = sinkConfig.getString(DorisConfigOptions.JDBC_URL);
        Preconditions.checkNotNull(fenodes, "fenodes is empty in sink-conf");
        Preconditions.checkNotNull(user, "username is empty in sink-conf");
        Preconditions.checkNotNull(jdbcUrl, "jdbcurl is empty in sink-conf");
        DorisConnectionOptions.DorisConnectionOptionsBuilder builder = new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                .withFenodes(fenodes)
                .withUsername(user)
                .withPassword(passwd)
                .withJdbcUrl(jdbcUrl);
        return builder.build();
    }

    /**
     * create doris sink
     */
    public DorisSink<String> buildDorisSink(String table) {
        String fenodes = sinkConfig.getString(DorisConfigOptions.FENODES);
        String user = sinkConfig.getString(DorisConfigOptions.USERNAME);
        String passwd = sinkConfig.getString(DorisConfigOptions.PASSWORD, "");
        String labelPrefix = sinkConfig.getString(DorisConfigOptions.SINK_LABEL_PREFIX);

        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes(fenodes)
                .setTableIdentifier(database + "." + table)
                .setUsername(user)
                .setPassword(passwd);

        Properties pro = new Properties();
        //default json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        //customer stream load properties
        Properties streamLoadProp = DorisConfigOptions.getStreamLoadProp(sinkConfig.toMap());
        pro.putAll(streamLoadProp);
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder()
                .setLabelPrefix(String.join("-", labelPrefix, database, table))
                .setStreamLoadProp(pro);

        sinkConfig.getOptional(DorisConfigOptions.SINK_ENABLE_DELETE).ifPresent(executionBuilder::setDeletable);
        sinkConfig.getOptional(DorisConfigOptions.SINK_BUFFER_COUNT).ifPresent(executionBuilder::setBufferCount);
        sinkConfig.getOptional(DorisConfigOptions.SINK_BUFFER_SIZE).ifPresent(executionBuilder::setBufferSize);
        sinkConfig.getOptional(DorisConfigOptions.SINK_CHECK_INTERVAL).ifPresent(executionBuilder::setCheckInterval);
        sinkConfig.getOptional(DorisConfigOptions.SINK_MAX_RETRIES).ifPresent(executionBuilder::setMaxRetries);

        boolean enable2pc = sinkConfig.getBoolean(DorisConfigOptions.SINK_ENABLE_2PC);
        if(!enable2pc){
            executionBuilder.disable2PC();
        }
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(JsonDebeziumSchemaSerializer.builder().setDorisOptions(dorisBuilder.build()).build())
                .setDorisOptions(dorisBuilder.build());
        return builder.build();
    }

    /**
     * Filter table that need to be synchronized
     */
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

    public static class TableNameConverter implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String prefix;
        private final String suffix;

        TableNameConverter(){
            this("","");
        }

        TableNameConverter(String prefix, String suffix) {
            this.prefix = prefix == null ? "" : prefix;
            this.suffix = suffix == null ? "" : suffix;
        }

        public String convert(String tableName) {
            return prefix + tableName + suffix;
        }
    }
}
