package org.apache.doris.flink.tools.cdc.mongodb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.doris.flink.tools.cdc.DatabaseSync.TableNameConverter;
import org.apache.doris.flink.tools.cdc.ParsingProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoParsingProcessFunction extends ParsingProcessFunction {
    private static final Logger LOG = LoggerFactory.getLogger(MongoParsingProcessFunction.class);

    public MongoParsingProcessFunction(TableNameConverter converter) {
        super(converter);
    }

    @Override
    protected String getRecordTableName(String record) throws Exception {
        JsonNode jsonNode = objectMapper.readValue(record, JsonNode.class);
        if (jsonNode.get("ns") == null || jsonNode.get("ns") instanceof NullNode) {
            LOG.error("Failed to get cdc namespace");
            throw new RuntimeException();
        }
        JsonNode nameSpace = jsonNode.get("ns");
        return extractJsonNode(nameSpace, "coll");
    }
}
