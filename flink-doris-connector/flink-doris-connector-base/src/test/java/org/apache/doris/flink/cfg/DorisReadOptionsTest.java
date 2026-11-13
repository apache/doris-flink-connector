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

package org.apache.doris.flink.cfg;

import org.junit.Assert;
import org.junit.Test;

public class DorisReadOptionsTest {

    @Test
    public void testEquals() {

        DorisReadOptions readOptions =
                DorisReadOptions.builder()
                        .setReadFields("xxx")
                        .setRequestReadTimeoutMs(1000)
                        .setRequestQueryTimeoutS(2000)
                        .setRequestTabletSize(1)
                        .setRequestBatchSize(10)
                        .setFilterQuery("a=1")
                        .setRequestRetries(1)
                        .setRequestConnectTimeoutMs(1000)
                        .setDeserializeArrowAsync(true)
                        .setDeserializeQueueSize(2)
                        .setExecMemLimit(1024L)
                        .setUseOldApi(true)
                        .build();

        DorisReadOptions.Builder builder =
                DorisReadOptions.builder()
                        .setReadFields("xxx")
                        .setRequestReadTimeoutMs(1000)
                        .setRequestQueryTimeoutS(2000)
                        .setRequestTabletSize(1)
                        .setRequestBatchSize(10)
                        .setFilterQuery("a=1")
                        .setRequestRetries(1)
                        .setRequestConnectTimeoutMs(1000)
                        .setDeserializeArrowAsync(true)
                        .setDeserializeQueueSize(2)
                        .setExecMemLimit(1024L)
                        .setUseOldApi(true);

        Assert.assertNotEquals(readOptions, null);
        Assert.assertEquals(readOptions, readOptions);
        Assert.assertEquals(readOptions, builder.build());

        builder.setReadFields("yyy");
        Assert.assertNotEquals(readOptions, builder.build());
        builder.setReadFields("xxx");

        builder.setRequestReadTimeoutMs(2000);
        Assert.assertNotEquals(readOptions, builder.build());
        builder.setRequestReadTimeoutMs(1000);

        builder.setRequestQueryTimeoutS(3000);
        Assert.assertNotEquals(readOptions, builder.build());
        builder.setRequestQueryTimeoutS(2000);

        builder.setRequestTabletSize(3);
        Assert.assertNotEquals(readOptions, builder.build());
        builder.setRequestTabletSize(1);

        builder.setRequestBatchSize(12);
        Assert.assertNotEquals(readOptions, builder.build());
        builder.setRequestBatchSize(10);

        builder.setFilterQuery("a=2");
        Assert.assertNotEquals(readOptions, builder.build());
        builder.setFilterQuery("a=1");

        builder.setRequestRetries(3);
        Assert.assertNotEquals(readOptions, builder.build());
        builder.setRequestRetries(1);

        builder.setRequestConnectTimeoutMs(3);
        Assert.assertNotEquals(readOptions, builder.build());
        builder.setRequestConnectTimeoutMs(1000);

        builder.setDeserializeArrowAsync(false);
        Assert.assertNotEquals(readOptions, builder.build());
        builder.setDeserializeArrowAsync(true);

        builder.setDeserializeQueueSize(3);
        Assert.assertNotEquals(readOptions, builder.build());
        builder.setDeserializeQueueSize(2);

        builder.setExecMemLimit(1025L);
        Assert.assertNotEquals(readOptions, builder.build());
        builder.setExecMemLimit(1024L);

        builder.setUseOldApi(false);
        Assert.assertNotEquals(readOptions, builder.build());
        builder.setUseOldApi(true);
    }
}
