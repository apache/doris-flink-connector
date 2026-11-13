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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.flink.sink.writer.ChangeEvent;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;

import java.io.IOException;
import java.util.Map;

/**
 * When cdc connector captures data changes from the source database you need to inherit this class
 * to complete the synchronized data changes to Doris schema. Supports data messages serialized to
 * json
 */
public abstract class CdcDataChange implements ChangeEvent {

    public abstract DorisRecord serialize(String record, JsonNode recordRoot, String op)
            throws IOException;

    protected abstract Map<String, Object> extractBeforeRow(JsonNode record);

    protected abstract Map<String, Object> extractAfterRow(JsonNode record);
}
