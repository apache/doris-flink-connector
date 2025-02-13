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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.util.CollectionUtil;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.CopyLoadException;
import org.apache.doris.flink.sink.HttpUtil;
import org.apache.doris.flink.sink.ResponseUtil;
import org.apache.doris.flink.sink.copy.models.BaseResponse;
import org.apache.doris.flink.sink.copy.models.CopyIntoResp;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DorisCopyCommitter implements Committer<DorisCopyCommittable>, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DorisCopyCommitter.class);
    private static final String commitPattern = "http://%s/copy/query";
    private static final int SUCCESS = 0;
    private static final String FAIL = "1";
    private ObjectMapper objectMapper = new ObjectMapper();
    private final DorisOptions dorisOptions;
    private HttpClientBuilder httpClientBuilder = new HttpUtil().getHttpClientBuilderForCopyBatch();
    int maxRetry;

    public DorisCopyCommitter(DorisOptions dorisOptions, int maxRetry) {
        this.dorisOptions = dorisOptions;
        this.maxRetry = maxRetry;
    }

    public DorisCopyCommitter(
            DorisOptions dorisOptions, int maxRetry, HttpClientBuilder httpClientBuilder) {
        this.dorisOptions = dorisOptions;
        this.maxRetry = maxRetry;
        this.httpClientBuilder = httpClientBuilder;
    }

    @Override
    public void commit(Collection<CommitRequest<DorisCopyCommittable>> committableList)
            throws IOException, InterruptedException {
        for (CommitRequest<DorisCopyCommittable> committable : committableList) {
            commitTransaction(committable.getCommittable());
        }
    }

    private void commitTransaction(DorisCopyCommittable committable) throws IOException {
        String hostPort = committable.getHostPort();
        String copySQL = committable.getCopySQL();

        int statusCode = -1;
        String reasonPhrase = null;
        int retry = 0;
        Map<String, String> params = new HashMap<>();
        params.put("sql", copySQL);
        boolean success = false;
        String loadResult = "";
        long start = System.currentTimeMillis();
        while (retry++ <= maxRetry) {
            LOG.info("commit with copy sql: {}", copySQL);
            HttpPostBuilder postBuilder = new HttpPostBuilder();
            postBuilder
                    .setUrl(String.format(commitPattern, hostPort))
                    .baseAuth(dorisOptions.getUsername(), dorisOptions.getPassword())
                    .setEntity(new StringEntity(objectMapper.writeValueAsString(params)));
            try (CloseableHttpClient httpClient = httpClientBuilder.build()) {
                try (CloseableHttpResponse response = httpClient.execute(postBuilder.build())) {
                    statusCode = response.getStatusLine().getStatusCode();
                    reasonPhrase = response.getStatusLine().getReasonPhrase();
                    if (statusCode != 200) {
                        LOG.warn(
                                "commit failed with status {} {}, reason {}",
                                statusCode,
                                hostPort,
                                reasonPhrase);
                    } else if (response.getEntity() != null) {
                        loadResult = EntityUtils.toString(response.getEntity());
                        success = handleCommitResponse(loadResult);
                        if (success) {
                            LOG.info(
                                    "commit success cost {}ms, response is {}",
                                    System.currentTimeMillis() - start,
                                    loadResult);
                            break;
                        } else {
                            LOG.warn("commit failed, retry again");
                        }
                    }
                } catch (IOException e) {
                    LOG.error("commit error : ", e);
                }
            }
        }

        if (!success) {
            LOG.error(
                    "commit error with status {}, reason {}, response {}",
                    statusCode,
                    reasonPhrase,
                    loadResult);
            String copyErrMsg =
                    String.format(
                            "commit error, status: %d, reason: %s, response: %s, copySQL: %s",
                            statusCode, reasonPhrase, loadResult, committable.getCopySQL());
            throw new CopyLoadException(copyErrMsg);
        }
    }

    public boolean handleCommitResponse(String loadResult) throws IOException {
        BaseResponse baseResponse =
                objectMapper.readValue(loadResult, new TypeReference<BaseResponse>() {});
        if (baseResponse.getCode() == SUCCESS && baseResponse.getData() instanceof Map) {
            CopyIntoResp dataResp =
                    objectMapper.convertValue(baseResponse.getData(), CopyIntoResp.class);
            // Returning code means there is an exception, and returning result means success
            if (FAIL.equals(dataResp.getDataCode())) {
                LOG.error("copy into execute failed, reason:{}", loadResult);
                return false;
            } else {
                Map<String, String> result = dataResp.getResult();
                if (CollectionUtil.isNullOrEmpty(result)
                        || (!result.get("state").equals("FINISHED")
                                && !ResponseUtil.isCopyCommitted(result.get("msg")))) {
                    LOG.error("copy into load failed, reason:{}", loadResult);
                    return false;
                } else {
                    return true;
                }
            }
        } else {
            LOG.error("commit failed, reason:{}", loadResult);
            return false;
        }
    }

    @Override
    public void close() throws IOException {}
}
