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

package org.apache.doris.flink.sink.writer;

import org.apache.doris.flink.sink.batch.RecordWithMeta;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.RecordWithMetaSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestRecordWithMetaSerializer {

    @Test
    public void testSerialize() throws IOException {
        RecordWithMetaSerializer serializer = new RecordWithMetaSerializer();
        RecordWithMeta record = new RecordWithMeta();
        record.setRecord("doris,1");
        DorisRecord serialize = serializer.serialize(record);
        Assert.assertNull(null, serialize);

        record.setDatabase("database");
        record.setTable("table");
        serialize = serializer.serialize(record);
        DorisRecord expected = DorisRecord.of("database", "table", "doris,1".getBytes());
        Assert.assertEquals(expected.getDatabase(), serialize.getDatabase());
        Assert.assertEquals(expected.getTable(), serialize.getTable());
        Assert.assertEquals(new String(expected.getRow()), new String(serialize.getRow()));
        Assert.assertEquals(expected.getTableIdentifier(), serialize.getTableIdentifier());

        Assert.assertNull(serializer.serialize(new RecordWithMeta(null, "table", "doris,1")));
        Assert.assertNull(serializer.serialize(new RecordWithMeta("database", "table", null)));
    }
}
