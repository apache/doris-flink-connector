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

package org.apache.doris.flink.tools.cdc.mongodb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import org.apache.doris.flink.tools.cdc.DatabaseSync.TableNameConverter;
import org.apache.doris.flink.tools.cdc.ParsingProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoParsingProcessFunction extends ParsingProcessFunction {
    private static final Logger LOG = LoggerFactory.getLogger(MongoParsingProcessFunction.class);

    public MongoParsingProcessFunction(String databaseName, TableNameConverter converter) {
        super(databaseName, converter);
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
