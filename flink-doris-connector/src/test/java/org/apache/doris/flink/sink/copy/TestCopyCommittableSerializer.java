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

import org.junit.Assert;
import org.junit.Test;

public class TestCopyCommittableSerializer {

    @Test
    public void testSerialize() throws Exception {
        DorisCopyCommittable expectCommittable =
                new DorisCopyCommittable(
                        "fe:8040",
                        "COPY INTO db.table FROM @u FILES=('label_0_1') FILE_FORMAT=('type'='csv','line_delimiter'='\n','column_separator'=',')");
        CopyCommittableSerializer serializer = new CopyCommittableSerializer();
        DorisCopyCommittable committable =
                serializer.deserialize(1, serializer.serialize(expectCommittable));
        Assert.assertEquals(expectCommittable, committable);
    }
}
