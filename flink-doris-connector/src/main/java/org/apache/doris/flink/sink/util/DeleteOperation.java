package org.apache.doris.flink.sink.util;

import java.util.Map;

import static org.apache.doris.flink.sink.writer.LoadConstants.DORIS_DELETE_SIGN;

public class DeleteOperation {
    public static void addDeleteSign(Map<String, Object> valueMap, boolean delete) {
        if (delete) {
            valueMap.put(DORIS_DELETE_SIGN, "1");
        } else {
            valueMap.put(DORIS_DELETE_SIGN, "0");
        }
    }
}
