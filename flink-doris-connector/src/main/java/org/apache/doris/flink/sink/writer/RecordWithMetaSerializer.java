package org.apache.doris.flink.sink.writer;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.batch.RecordWithMeta;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class RecordWithMetaSerializer implements DorisRecordSerializer<RecordWithMeta>{
    private static final Logger LOG = LoggerFactory.getLogger(RecordWithMetaSerializer.class);

    @Override
    public Tuple2<String, byte[]> serialize(RecordWithMeta record) throws IOException {
        if(StringUtils.isBlank(record.getTable())
                || StringUtils.isBlank(record.getDatabase())
                || record.getRecord() == null){
            LOG.warn("Record or meta format is incorrect, ignore record db:{}, table:{}, row:{}",
                    record.getDatabase(), record.getTable(), record.getRecord());
            return null;
        }
        String tableKey = record.getDatabase() + "." + record.getTable();
        return Tuple2.of(tableKey, record.getRecord().getBytes(StandardCharsets.UTF_8));
    }
}
