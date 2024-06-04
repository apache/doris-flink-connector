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

package org.apache.doris.flink.sink;

import org.junit.Assert;
import org.junit.Test;

public class TestDorisCommittable {

    @Test
    public void testDorisCommittableEqual() {
        DorisCommittable dorisCommittable = new DorisCommittable("127.0.0.1:8710", "test", 0);
        Assert.assertNotEquals(dorisCommittable, null);

        DorisCommittable committable2 = new DorisCommittable("127.0.0.1:8710", "test", 0);
        Assert.assertEquals(dorisCommittable, committable2);
        Assert.assertEquals(dorisCommittable, dorisCommittable);
        Assert.assertNotEquals(dorisCommittable, null);

        committable2 = new DorisCommittable("127.0.0.1:8710", "test", 1);
        Assert.assertNotEquals(dorisCommittable, committable2);

        committable2 = new DorisCommittable("127.0.0.1:8710", "test1", 0);
        Assert.assertNotEquals(dorisCommittable, committable2);

        committable2 = new DorisCommittable("127.0.0.2:8710", "test", 0);
        Assert.assertNotEquals(dorisCommittable, committable2);
    }
}
