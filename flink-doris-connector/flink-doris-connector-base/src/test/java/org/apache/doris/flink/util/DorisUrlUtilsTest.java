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

package org.apache.doris.flink.util;

import org.junit.Assert;
import org.junit.Test;

public class DorisUrlUtilsTest {
    @Test
    public void testEncodePathSegment() {
        Assert.assertEquals("table_1", DorisUrlUtils.encodePathSegment("table_1"));
        Assert.assertEquals(
                "ods_%E6%96%B0%E5%88%B8%E8%A1%A8_copy1",
                DorisUrlUtils.encodePathSegment("ods_新券表_copy1"));
        Assert.assertEquals("a%20b", DorisUrlUtils.encodePathSegment("a b"));
        Assert.assertEquals("a%25b", DorisUrlUtils.encodePathSegment("a%b"));
    }

    @Test
    public void testBuildHttpUrl() {
        Assert.assertEquals(
                "http://127.0.0.1:8030/api/ods/ods_%E6%96%B0%E5%88%B8%E8%A1%A8_copy1/_schema",
                DorisUrlUtils.buildHttpUrl(
                        "127.0.0.1:8030", "api", "ods", "ods_新券表_copy1", "_schema"));
    }
}
