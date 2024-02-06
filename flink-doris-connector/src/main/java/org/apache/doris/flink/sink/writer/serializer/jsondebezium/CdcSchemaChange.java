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

package org.apache.doris.flink.sink.writer.serializer.jsondebezium;

import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.flink.sink.writer.ChangeEvent;

import java.io.IOException;
import java.util.Map;

public abstract class CdcSchemaChange implements ChangeEvent {

    public DorisOptions dorisOptions;

    public Map<String, String> tableMapping;

    public abstract String extractDatabase(JsonNode record);

    public abstract String extractTable(JsonNode record);

    public abstract void init(JsonNode recordRoot);

    public abstract boolean schemaChange(JsonNode recordRoot) throws IOException;

    public abstract String getCdcTableIdentifier(JsonNode record);

    @VisibleForTesting
    public abstract void setSchemaChangeManager(SchemaChangeManager schemaChangeManager);

    public String getDorisTableIdentifier(String cdcTableIdentifier) {
        if (!StringUtils.isNullOrWhitespaceOnly(dorisOptions.getTableIdentifier())) {
            return dorisOptions.getTableIdentifier();
        }
        if (!CollectionUtil.isNullOrEmpty(tableMapping)
                && !StringUtils.isNullOrWhitespaceOnly(cdcTableIdentifier)
                && tableMapping.get(cdcTableIdentifier) != null) {
            return tableMapping.get(cdcTableIdentifier);
        }
        return null;
    }
}
