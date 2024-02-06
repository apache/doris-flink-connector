package org.apache.doris.flink.tools.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.tools.cdc.mongodb.MongoDBDatabaseSync;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CdcMongoSyncDatabaseCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //        env.setParallelism(1);

        Map<String, String> flinkMap = new HashMap<>();
        flinkMap.put("execution.checkpointing.interval", "3s");
        flinkMap.put("pipeline.operator-chaining", "false");
        flinkMap.put("parallelism.default", "1");

        String database = "test";
        String tablePrefix = "";
        String tableSuffix = "";

        Configuration configuration = Configuration.fromMap(flinkMap);
        env.configure(configuration);
        Map<String, String> mongoConfig = new HashMap<>();
        mongoConfig.put("database", "test");
        mongoConfig.put("hosts", "127.0.0.1:27018");
        mongoConfig.put("username", "flinkuser");
        // mysqlConfig.put("password","");
        mongoConfig.put("password", "flinkpwd");
        //                mongoConfig.put("scan.startup.mode", "latest-offset");
        mongoConfig.put("scan.startup.mode", "initial");
        mongoConfig.put("mongo-cdc.create-sample-percent", "1");
        Configuration config = Configuration.fromMap(mongoConfig);

        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put("fenodes", "10.16.10.6:8036");
        // sinkConfig.put("benodes","10.20.30.1:8040, 10.20.30.2:8040, 10.20.30.3:8040");
        sinkConfig.put("username", "root");
        sinkConfig.put("password", "12345");
        sinkConfig.put("jdbc-url", "jdbc:mysql://10.16.10.6:9036");
        sinkConfig.put("sink.label-prefix", UUID.randomUUID().toString());
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put("replication_num", "1");
        tableConfig.put("table-buckets", ".*:1");
        String includingTables = "number_test";
        //        String includingTables = "a_.*|b_.*|c";
        String excludingTables = "";
        String multiToOneOrigin = "a_.*|b_.*";
        String multiToOneTarget = "a|b";
        boolean ignoreDefaultValue = false;
        boolean useNewSchemaChange = false;
        DatabaseSync databaseSync = new MongoDBDatabaseSync();
        databaseSync
                .setEnv(env)
                .setDatabase(database)
                .setConfig(config)
                .setTablePrefix(tablePrefix)
                .setTableSuffix(tableSuffix)
                .setIncludingTables(includingTables)
                .setExcludingTables(excludingTables)
                .setMultiToOneOrigin(multiToOneOrigin)
                .setMultiToOneTarget(multiToOneTarget)
                .setIgnoreDefaultValue(ignoreDefaultValue)
                .setSinkConfig(sinkConf)
                .setTableConfig(tableConfig)
                .setCreateTableOnly(false)
                .setNewSchemaChange(useNewSchemaChange)
                .create();
        databaseSync.build();
        env.execute(String.format("Mongo-Doris Database Sync: %s", database));
    }
}
