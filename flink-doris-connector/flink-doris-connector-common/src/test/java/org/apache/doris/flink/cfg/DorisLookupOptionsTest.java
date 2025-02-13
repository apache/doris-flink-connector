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

public class DorisLookupOptionsTest {

    @Test
    public void testEquals() {
        DorisLookupOptions lookupOptions =
                DorisLookupOptions.builder()
                        .setJdbcReadBatchQueueSize(2)
                        .setAsync(true)
                        .setMaxRetryTimes(2)
                        .setJdbcReadThreadSize(2)
                        .setCacheExpireMs(10000)
                        .setCacheMaxSize(1000)
                        .setJdbcReadBatchSize(2021)
                        .build();

        DorisLookupOptions.Builder builder =
                DorisLookupOptions.builder()
                        .setJdbcReadBatchQueueSize(2)
                        .setAsync(true)
                        .setMaxRetryTimes(2)
                        .setJdbcReadThreadSize(2)
                        .setCacheExpireMs(10000)
                        .setCacheMaxSize(1000)
                        .setJdbcReadBatchSize(2021);

        Assert.assertNotEquals(lookupOptions, null);
        Assert.assertEquals(lookupOptions, lookupOptions);
        Assert.assertEquals(lookupOptions, builder.build());

        builder.setJdbcReadBatchSize(3);
        Assert.assertNotEquals(lookupOptions, builder.build());
        builder.setJdbcReadBatchSize(2021);

        builder.setAsync(false);
        Assert.assertNotEquals(lookupOptions, builder.build());
        builder.setAsync(true);

        builder.setMaxRetryTimes(33);
        Assert.assertNotEquals(lookupOptions, builder.build());
        builder.setMaxRetryTimes(2);

        builder.setJdbcReadThreadSize(3);
        Assert.assertNotEquals(lookupOptions, builder.build());
        builder.setJdbcReadThreadSize(2);

        builder.setCacheExpireMs(3);
        Assert.assertNotEquals(lookupOptions, builder.build());
        builder.setCacheExpireMs(10000);

        builder.setCacheMaxSize(4);
        Assert.assertNotEquals(lookupOptions, builder.build());
        builder.setCacheMaxSize(1000);

        builder.setJdbcReadBatchSize(400);
        Assert.assertNotEquals(lookupOptions, builder.build());
        builder.setJdbcReadBatchSize(2021);
    }
}
