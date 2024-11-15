package org.apache.doris.flink.lookup;

import org.apache.doris.flink.container.AbstractITCaseService;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.source.DorisSourceITCase;
import org.apache.doris.flink.table.DorisConfigOptions;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DorisLookupTableITCase extends AbstractITCaseService {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSourceITCase.class);
    private static final String DATABASE = "test_lookup";
    private static final String TABLE_READ_TBL = "tbl_read_tbl";

    @Test
    public void testLookupTable() throws Exception {
        initializeTable(TABLE_READ_TBL);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String datagenDDL = "CREATE TABLE datagen (" +
                " `id` INT," +
                " `proctime` AS PROCTIME()" +
                ") WITH (" +
                " 'connector' = 'datagen'," +
                " 'rows-per-second' = '1'," +
                " 'fields.id.kind' = 'random'," +
                " 'fields.id.min' = '1'," +
                " 'fields.id.max' = '10'" +
                ")";
        tEnv.executeSql(datagenDDL);
        String lookupDDL = String.format("CREATE TABLE `doris_lookup`(" +
                        "  `id` INTEGER," +
                        "  `tinyintColumn` TINYINT," +
                        "  `smallintColumn` SMALLINT," +
                        "  `bigintColumn` BIGINT," +
                        "  PRIMARY KEY (`id`) NOT ENFORCED" +
                        ")  WITH (" +
                        "'connector' = '" + DorisConfigOptions.IDENTIFIER + "'," +
                        "'fenodes' = '%s'," +
                        "'jdbc-url' = '%s'," +
                        "'table.identifier' = '%s'," +
                        "'username' = '%s'," +
                        "'password' = '%s'," +
                        "'lookup.cache.max-rows' = '100'" +
                        ")",
                getFenodes(),
                getDorisQueryUrl(),
                DATABASE + "." + TABLE_READ_TBL,
                getDorisUsername(),
                getDorisPassword());
        tEnv.executeSql(lookupDDL);
        tEnv.executeSql("select datagen.id," +
                "tinyintColumn," +
                "smallintColumn," +
                "bigintColumn" +
                " from `datagen`" +
                " inner join `doris_lookup` FOR SYSTEM_TIME AS OF datagen.proctime on datagen.id = doris_lookup.id").print();
        env.execute("Flink doris lookup test");
    }

    private void initializeTable(String table) {
        ContainerUtils.executeSQLStatement(
                getDorisQueryConnection(),
                LOG,
                String.format("CREATE DATABASE IF NOT EXISTS %s", DATABASE),
                String.format("DROP TABLE IF EXISTS %s.%s", DATABASE, table),
                String.format(
                        "CREATE TABLE %s.%s ( \n"
                                + "`id` int(11),\n"
                                + "`tinyintColumn` tinyint(4),\n"
                                + "`smallintColumn` smallint(6),\n"
                                + "`bigintColumn` bigint(20),\n"
                                + ") DISTRIBUTED BY HASH(`id`) BUCKETS 10\n"
                                + "PROPERTIES (\n"
                                + "\"replication_num\" = \"1\"\n"
                                + ")\n",
                        DATABASE, table),
                String.format("insert into %s.%s  values (1,97,27479,8670353564751764000)", DATABASE, table),
                String.format("insert into %s.%s  values (2,79,17119,-4381380624467725000)", DATABASE, table),
                String.format("insert into %s.%s  values (3,-106,-14878,1466614815449373200)", DATABASE, table));
    }
}
