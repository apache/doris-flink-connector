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

import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.ProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.protocol.HttpCoreContext;
import org.junit.Assert;
import org.junit.Test;

public class TestHttpUtil {

    @Test
    public void testRedirectStrategyKeepsPutRedirectable() throws ProtocolException {
        HttpResponse response =
                new BasicHttpResponse(HttpVersion.HTTP_1_1, 307, "Temporary Redirect");
        response.setHeader("location", "http://127.0.0.1:8040/api/db/tbl/_stream_load");

        Assert.assertTrue(
                HttpUtil.getRedirectPutStrategy()
                        .isRedirected(
                                new HttpPut("https://127.0.0.1:8030/api/db/tbl/_stream_load"),
                                response,
                                new HttpCoreContext()));
        Assert.assertTrue(
                HttpUtil.getRedirectPutStrategy()
                        .isRedirected(
                                new HttpGet("https://127.0.0.1:8030/api/backends"),
                                response,
                                new HttpCoreContext()));
    }
}
