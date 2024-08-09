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

package org.apache.doris.flink.sink.copy;

import org.apache.doris.flink.catalog.doris.DorisSchemaFactory;
import org.apache.doris.flink.cfg.DorisExecutionOptions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

import static org.apache.doris.flink.sink.writer.LoadConstants.COLUMNS_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.FIELD_DELIMITER_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.FORMAT_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.JSON;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.READ_JSON_BY_LINE;

public class CopySQLBuilder {
    private static final String COPY_SYNC = "copy.async";
    private static final String COPY_DELETE = "copy.use_delete_sign";
    private static final String STRIP_OUT_ARRAY = "strip_outer_array";
    private final DorisExecutionOptions executionOptions;
    private final String tableIdentifier;
    private final List<String> fileList;
    private Properties properties;

    public CopySQLBuilder(
            String tableIdentifier, DorisExecutionOptions executionOptions, List<String> fileList) {
        this.tableIdentifier = tableIdentifier;
        this.executionOptions = executionOptions;
        this.fileList = fileList;
        this.properties = executionOptions.getStreamLoadProp();
    }

    public String buildCopySQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ")
                .append(DorisSchemaFactory.quoteTableIdentifier(tableIdentifier))
                .append(" FROM @~('{")
                .append(String.join(",", fileList))
                .append("}') ")
                .append("PROPERTIES (");

        // copy into must be sync
        properties.put(COPY_SYNC, false);
        if (executionOptions.getDeletable()) {
            properties.put(COPY_DELETE, true);
        }

        if (JSON.equals(properties.getProperty(FORMAT_KEY))) {
            properties.put(STRIP_OUT_ARRAY, false);
        }

        properties.remove(READ_JSON_BY_LINE);
        properties.remove(COLUMNS_KEY);
        StringJoiner props = new StringJoiner(",");
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = concatPropPrefix(String.valueOf(entry.getKey()));
            String value = String.valueOf(entry.getValue());
            String prop = String.format("'%s'='%s'", key, value);
            props.add(prop);
        }
        sb.append(props).append(")");
        return sb.toString();
    }

    static final List<String> PREFIX_LIST =
            Arrays.asList(FIELD_DELIMITER_KEY, LINE_DELIMITER_KEY, STRIP_OUT_ARRAY);

    private String concatPropPrefix(String key) {
        if (PREFIX_LIST.contains(key)) {
            return "file." + key;
        }
        if (FORMAT_KEY.equals(key)) {
            return "file.type";
        }
        return key;
    }
}
