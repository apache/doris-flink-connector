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

package org.apache.doris.flink.sink.writer.tvf;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class S3TvfCommittableSerializerTest {

    @Test
    public void testRoundTrip() throws Exception {
        S3TvfCommittable committable =
                new S3TvfCommittable(
                        7L,
                        "db",
                        "tbl",
                        "flink_tbl_7",
                        Arrays.asList("prefix_tbl_0_7_0.json", "prefix_tbl_0_7_1.json"),
                        Arrays.asList("id", "name"),
                        true);
        S3TvfCommittableSerializer serializer = new S3TvfCommittableSerializer();

        byte[] serialized = serializer.serialize(committable);
        S3TvfCommittable restored = serializer.deserialize(serializer.getVersion(), serialized);

        Assert.assertEquals(committable, restored);
    }
}
