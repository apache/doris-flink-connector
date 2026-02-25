package org.apache.doris.flink.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.container.AbstractITCaseService;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.doris.flink.source.DorisSourceITCase.checkResultInAnyOrder;
import static org.apache.doris.flink.source.DorisSourceITCase.initializeTable;

public class DorisSourceITCaseForOldApi extends AbstractITCaseService {

    private static final String DATABASE = "test_source";
    public static final String TABLE_READ_OLD_API = "tbl_read_old_api";

    @Test
    public void testOldSourceApi() throws Exception {
        initializeTable(TABLE_READ_OLD_API, DataModel.UNIQUE);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        Properties properties = new Properties();
        properties.put("fenodes", getFenodes());
        properties.put("username", getDorisUsername());
        properties.put("password", getDorisPassword());
        properties.put("table.identifier", DATABASE + "." + TABLE_READ_OLD_API);
        DorisStreamOptions options = new DorisStreamOptions(properties);

        List<String> actual = new ArrayList<>();
        try (CloseableIterator<List<?>> iterator =
                env.addSource(
                                new DorisSourceFunction(
                                        options, new SimpleListDeserializationSchema()))
                        .executeAndCollect()) {
            while (iterator.hasNext()) {
                actual.add(iterator.next().toString());
            }
        }
        List<String> expected = Arrays.asList("[doris, 18]", "[flink, 10]", "[apache, 12]");
        checkResultInAnyOrder("testOldSourceApi", expected.toArray(), actual.toArray());
    }
}
