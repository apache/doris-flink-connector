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

package org.apache.doris.flink.sink.writer.serializer;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.sink.batch.RecordWithMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@PublicEvolving
public class RecordWithMetaSerializer implements DorisRecordSerializer<RecordWithMeta> {
    private static final Logger LOG = LoggerFactory.getLogger(RecordWithMetaSerializer.class);

    @Override
    public DorisRecord serialize(RecordWithMeta record) throws IOException {
        if (StringUtils.isBlank(record.getTable())
                || StringUtils.isBlank(record.getDatabase())
                || record.getRecord() == null) {
            LOG.warn(
                    "Record or meta format is incorrect, ignore record db:{}, table:{}, row:{}",
                    record.getDatabase(),
                    record.getTable(),
                    record.getRecord());
            return null;
        }
        return DorisRecord.of(
                record.getDatabase(),
                record.getTable(),
                record.getRecord().getBytes(StandardCharsets.UTF_8));
    }
}
