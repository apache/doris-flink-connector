package org.apache.doris.flink.lookup;

import org.apache.doris.flink.container.AbstractITCaseService;
import org.apache.doris.flink.container.ContainerUtils;
import org.apache.doris.flink.table.DorisConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DorisLookupTableITCase extends AbstractITCaseService {

    private static final Logger LOG = LoggerFactory.getLogger(DorisLookupTableITCase.class);
    private static final String DATABASE = "test_lookup";
    private static final String TABLE_READ_TBL = "tbl_read_tbl";

    @Test
    public void testLookupTable() throws Exception {
        initializeTable(TABLE_READ_TBL);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        DataStreamSource<Integer> sourceStream = env.<Integer>fromElements(1, 2, 3, 4);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Schema schema = Schema.newBuilder()
                .column("f0", DataTypes.INT())
                .columnByExpression("proctime", "PROCTIME()")
                .build();
        Table table = tEnv.fromDataStream(sourceStream, schema);
        tEnv.createTemporaryView("source", table);

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
        TableResult tableResult = tEnv.executeSql("select source.f0," +
                "tinyintColumn," +
                "smallintColumn," +
                "bigintColumn" +
                " from `source`" +
                " inner join `doris_lookup` FOR SYSTEM_TIME AS OF source.proctime on source.f0 = doris_lookup.id");

        List<String> actual = new ArrayList<>();
        try (CloseableIterator<Row> iterator = tableResult.collect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }

        String[] expected = new String[]{"+I[1, 97, 27479, 8670353564751764000]", "+I[2, 79, 17119, -4381380624467725000]", "+I[3, -106, -14878, 1466614815449373200]"};
        assertEqualsInAnyOrder(Arrays.asList(expected), Arrays.asList(actual.toArray()));
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
