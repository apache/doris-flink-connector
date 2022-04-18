package org.apache.doris.flink.deserialization;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.doris.flink.utils.FactoryMocks.PHYSICAL_TYPE;
import static org.junit.Assert.assertEquals;

public class RowDataDeserializationSchemaTest {

    @Test
    public void deserializeTest() throws Exception {
        List<String> records = Arrays.asList("flink","doris");
        SimpleCollector collector = new SimpleCollector();
        RowDataDeserializationSchema deserializationSchema = new RowDataDeserializationSchema(PHYSICAL_TYPE);
        for(String record : records){
            deserializationSchema.deserialize(Arrays.asList(record),collector);
        }

        List<String> expected =
                Arrays.asList(
                        "+I(flink)",
                        "+I(doris)");

        List<String> actual =
                collector.list.stream().map(Object::toString).collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    private static class SimpleCollector implements Collector<RowData> {
        private List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
