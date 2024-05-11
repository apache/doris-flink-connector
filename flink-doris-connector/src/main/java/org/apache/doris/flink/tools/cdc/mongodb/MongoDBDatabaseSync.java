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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.ververica.cdc.connectors.base.options.SourceOptions;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSourceBuilder;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverterConfig;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.writer.serializer.DorisRecordSerializer;
import org.apache.doris.flink.sink.writer.serializer.MongoDBJsonDebeziumSchemaSerializer;
import org.apache.doris.flink.tools.cdc.DatabaseSync;
import org.apache.doris.flink.tools.cdc.ParsingProcessFunction;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.bson.Document;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.encodeValue;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class MongoDBDatabaseSync extends DatabaseSync {

    private static final String INITIAL_MODE = "initial";
    private static final String LATEST_OFFSET_MODE = "latest-offset";
    private static final String TIMESTAMP_MODE = "timestamp";
    public static final ConfigOption<Double> MONGO_CDC_CREATE_SAMPLE_PERCENT =
            ConfigOptions.key("schema.sample-percent")
                    .doubleType()
                    .defaultValue(0.2)
                    .withDescription("mongo cdc sample percent");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the Mongo database to monitor.");

    public MongoDBDatabaseSync() throws SQLException {}

    @Override
    public void registerDriver() throws SQLException {}

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public List<SourceSchema> getSchemaList() throws Exception {
        String databaseName = config.get(MongoDBSourceOptions.DATABASE);
        List<SourceSchema> schemaList = new ArrayList<>();
        MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();

        settingsBuilder.applyConnectionString(
                new ConnectionString(
                        buildConnectionString(
                                config.get(MongoDBSourceOptions.USERNAME),
                                config.get(MongoDBSourceOptions.PASSWORD),
                                config.get(MongoDBSourceOptions.SCHEME),
                                config.get(MongoDBSourceOptions.HOSTS),
                                config.get(MongoDBSourceOptions.CONNECTION_OPTIONS))));

        MongoClientSettings settings = settingsBuilder.build();
        Double samplePercent = config.get(MONGO_CDC_CREATE_SAMPLE_PERCENT);
        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
            MongoIterable<String> collectionNames = mongoDatabase.listCollectionNames();
            for (String collectionName : collectionNames) {
                if (!isSyncNeeded(collectionName)) {
                    continue;
                }
                MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
                Document firstDocument = collection.find().first();
                if (firstDocument == null) {
                    throw new IllegalStateException("No documents in collection to infer schema");
                }

                long totalDocuments = collection.countDocuments();
                long sampleSize = (long) Math.ceil(totalDocuments * samplePercent);
                ArrayList<Document> documents = sampleData(collection, sampleSize);
                MongoDBSchema mongoDBSchema =
                        new MongoDBSchema(documents, databaseName, collectionName, null);
                mongoDBSchema.setModel(DataModel.UNIQUE);
                schemaList.add(mongoDBSchema);
            }
        }

        return schemaList;
    }

    private ArrayList<Document> sampleData(MongoCollection<Document> collection, Long sampleNum) {
        ArrayList<Document> query = new ArrayList<>();
        query.add(new Document("$sample", new Document("size", sampleNum)));
        return collection.aggregate(query).into(new ArrayList<>());
    }

    private static String buildConnectionString(
            @Nullable String username,
            @Nullable String password,
            String scheme,
            String hosts,
            @Nullable String connectionOptions) {
        StringBuilder sb = new StringBuilder(scheme).append("://");
        if (StringUtils.isNotEmpty(username) && StringUtils.isNotEmpty(password)) {
            sb.append(encodeValue(username)).append(":").append(encodeValue(password)).append("@");
        }
        sb.append(checkNotNull(hosts));
        if (StringUtils.isNotEmpty(connectionOptions)) {
            sb.append("/?").append(connectionOptions);
        }
        return sb.toString();
    }

    @Override
    public DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env) {
        String hosts = config.get(MongoDBSourceOptions.HOSTS);
        String username = config.get(MongoDBSourceOptions.USERNAME);
        String password = config.get(MongoDBSourceOptions.PASSWORD);
        String database = config.get(MongoDBSourceOptions.DATABASE);
        String collection = config.get(MongoDBSourceOptions.COLLECTION);
        if (StringUtils.isBlank(collection)) {
            collection = config.get(TABLE_NAME);
        }
        MongoDBSourceBuilder<String> mongoDBSourceBuilder = MongoDBSource.builder();
        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(false, customConverterConfigs);

        mongoDBSourceBuilder
                .hosts(hosts)
                .username(username)
                .password(password)
                .databaseList(database)
                .collectionList(collection);

        String startupMode = config.get(SourceOptions.SCAN_STARTUP_MODE);
        switch (startupMode.toLowerCase()) {
            case INITIAL_MODE:
                mongoDBSourceBuilder.startupOptions(StartupOptions.initial());
                break;
            case LATEST_OFFSET_MODE:
                mongoDBSourceBuilder.startupOptions(StartupOptions.latest());
                break;
            case TIMESTAMP_MODE:
                mongoDBSourceBuilder.startupOptions(
                        StartupOptions.timestamp(
                                config.get(SourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)));
                break;
            default:
                throw new IllegalArgumentException("Unsupported startup mode: " + startupMode);
        }
        MongoDBSource<String> mongoDBSource = mongoDBSourceBuilder.deserializer(schema).build();
        return env.fromSource(mongoDBSource, WatermarkStrategy.noWatermarks(), "MongoDB Source");
    }

    @Override
    public ParsingProcessFunction buildProcessFunction() {
        return new MongoParsingProcessFunction(converter);
    }

    @Override
    public DorisRecordSerializer<String> buildSchemaSerializer(
            DorisOptions.Builder dorisBuilder, DorisExecutionOptions executionOptions) {
        return MongoDBJsonDebeziumSchemaSerializer.builder()
                .setDorisOptions(dorisBuilder.build())
                .setExecutionOptions(executionOptions)
                .setTableMapping(tableMapping)
                .setTableProperties(tableConfig)
                .setTargetDatabase(database)
                .build();
    }

    @Override
    public String getTableListPrefix() {
        return config.get(MongoDBSourceOptions.DATABASE);
    }
}
