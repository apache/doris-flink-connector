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

package org.apache.doris.flink.sink.committer;

import org.apache.flink.api.connector.sink.Committer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpPutBuilder;
import org.apache.doris.flink.sink.HttpUtil;
import org.apache.doris.flink.sink.ResponseUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.doris.flink.sink.LoadStatus.FAIL;

/**
 * The committer to commit transaction.
 */
public class DorisCommitter implements Committer<DorisCommittable> {
    private static final Logger LOG = LoggerFactory.getLogger(DorisCommitter.class);
    private static final String commitPattern = "http://%s/api/%s/_stream_load_2pc";
    private final CloseableHttpClient httpClient;
    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    int maxRetry;

    public DorisCommitter(DorisOptions dorisOptions, DorisReadOptions dorisReadOptions, int maxRetry) {
        this(dorisOptions, dorisReadOptions, maxRetry, new HttpUtil().getHttpClient());
    }

    public DorisCommitter(DorisOptions dorisOptions, DorisReadOptions dorisReadOptions, int maxRetry, CloseableHttpClient client) {
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.maxRetry = maxRetry;
        this.httpClient = client;
    }

    @Override
    public List<DorisCommittable> commit(List<DorisCommittable> committableList) throws IOException, InterruptedException {
        for (DorisCommittable committable : committableList) {
            commitTransaction(committable);
        }
        return Collections.emptyList();
    }

    private void commitTransaction(DorisCommittable committable) throws IOException {
        int statusCode = -1;
        String reasonPhrase = null;
        int retry = 0;
        String hostPort = committable.getHostPort();
        CloseableHttpResponse response = null;
        while (retry++ <= maxRetry) {
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder.setUrl(String.format(commitPattern, hostPort, committable.getDb()))
                    .baseAuth(dorisOptions.getUsername(), dorisOptions.getPassword())
                    .addCommonHeader()
                    .addTxnId(committable.getTxnID())
                    .setEmptyEntity()
                    .commit();
            try {
                response = httpClient.execute(putBuilder.build());
            } catch (IOException e) {
                hostPort = RestService.getBackend(dorisOptions, dorisReadOptions, LOG);
                continue;
            }
            statusCode = response.getStatusLine().getStatusCode();
            reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode != 200) {
                LOG.warn("commit failed with {}, reason {}", hostPort, reasonPhrase);
                hostPort = RestService.getBackend(dorisOptions, dorisReadOptions, LOG);
            } else {
                break;
            }
        }

        if (statusCode != 200) {
            throw new DorisRuntimeException("stream load error: " + reasonPhrase);
        }

        ObjectMapper mapper = new ObjectMapper();
        if (response.getEntity() != null) {
            String loadResult = EntityUtils.toString(response.getEntity());
            Map<String, String> res = mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {
            });
            if (res.get("status").equals(FAIL) && !ResponseUtil.isCommitted(res.get("msg"))) {
                throw new DorisRuntimeException("Commit failed " + loadResult);
            } else {
                LOG.info("load result {}", loadResult);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
