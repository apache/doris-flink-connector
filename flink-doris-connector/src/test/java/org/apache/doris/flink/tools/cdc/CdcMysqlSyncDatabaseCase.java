package org.apache.doris.flink.tools.cdc;

import org.apache.doris.flink.tools.cdc.mysql.MysqlDatabaseSync;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CdcMysqlSyncDatabaseCase {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        Map<String,String> flinkMap = new HashMap<>();
        flinkMap.put("execution.checkpointing.interval","10s");
        flinkMap.put("pipeline.operator-chaining","false");
        flinkMap.put("parallelism.default","1");


        Configuration configuration = Configuration.fromMap(flinkMap);
        env.configure(configuration);

        String database = "db1";
        String tablePrefix = "";
        String tableSuffix = "";
        Map<String,String> mysqlConfig = new HashMap<>();
        mysqlConfig.put("database-name","db1");
        mysqlConfig.put("hostname","127.0.0.1");
        mysqlConfig.put("port","3306");
        mysqlConfig.put("username","root");
        mysqlConfig.put("password","");
        Configuration config = Configuration.fromMap(mysqlConfig);

        Map<String,String> sinkConfig = new HashMap<>();
        sinkConfig.put("fenodes","127.0.0.1:8030");
        sinkConfig.put("username","root");
        sinkConfig.put("password","");
        sinkConfig.put("jdbc-url","jdbc:mysql://127.0.0.1:9030");
        sinkConfig.put("sink.label-prefix", UUID.randomUUID().toString());
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        Map<String,String> tableConfig = new HashMap<>();
        tableConfig.put("replication_num", "1");

        String includingTables = "tbl1|tbl2|tbl3";
        String excludingTables = "";
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        databaseSync.create(env,database,config,tablePrefix,tableSuffix,includingTables,excludingTables,sinkConf,tableConfig);
        databaseSync.build();
        env.execute(String.format("MySQL-Doris Database Sync: %s", database));

    }
}
