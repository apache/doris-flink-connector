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

import org.apache.flink.api.connector.sink2.Committer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpPutBuilder;
import org.apache.doris.flink.sink.HttpUtil;
import org.apache.doris.flink.sink.ResponseUtil;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.doris.flink.sink.LoadStatus.SUCCESS;

/** The committer to commit transaction. */
public class DorisCommitter implements Committer<DorisCommittable>, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(DorisCommitter.class);
    private static final String commitPattern = "http://%s/api/%s/_stream_load_2pc";
    private final CloseableHttpClient httpClient;
    private final DorisOptions dorisOptions;
    private final DorisReadOptions dorisReadOptions;
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final BackendUtil backendUtil;
    int maxRetry;
    CommitTolerance commitTolerance;

    public DorisCommitter(
            DorisOptions dorisOptions,
            DorisReadOptions dorisReadOptions,
            DorisExecutionOptions executionOptions) {
        this(
                dorisOptions,
                dorisReadOptions,
                executionOptions.getMaxRetries(),
                new HttpUtil().getHttpClient());

        commitTolerance = executionOptions.getCommitTolerance();
    }

    public DorisCommitter(
            DorisOptions dorisOptions,
            DorisReadOptions dorisReadOptions,
            int maxRetry,
            CloseableHttpClient client) {
        this.dorisOptions = dorisOptions;
        this.dorisReadOptions = dorisReadOptions;
        this.maxRetry = maxRetry;
        this.httpClient = client;
        this.backendUtil =
                StringUtils.isNotEmpty(dorisOptions.getBenodes())
                        ? new BackendUtil(dorisOptions.getBenodes())
                        : new BackendUtil(
                                RestService.getBackendsV2(dorisOptions, dorisReadOptions, LOG));
    }

    @Override
    public void commit(Collection<CommitRequest<DorisCommittable>> requests)
            throws IOException, InterruptedException {
        for (CommitRequest<DorisCommittable> request : requests) {
            commitTransaction(request);
        }
    }

    private void commitTransaction(CommitRequest<DorisCommittable> request) throws IOException {
        DorisCommittable committable = request.getCommittable();

        if (committable.getCommitTolerance() != null) {
            //  The commitTolerance variable of committable is the value passed
            //  after the last commit failed.
            commitTolerance = committable.getCommitTolerance();
        }
        // basic params
        HttpPutBuilder builder =
                new HttpPutBuilder()
                        .addCommonHeader()
                        .baseAuth(dorisOptions.getUsername(), dorisOptions.getPassword())
                        .addTxnId(committable.getTxnID())
                        .commit();

        // hostPort
        String hostPort = committable.getHostPort();
        LOG.info(
                "commit txn {} to host {}, tolerance is {}",
                committable.getTxnID(),
                hostPort,
                commitTolerance);
        int retry = 0;
        while (retry <= maxRetry) {
            // get latest-url
            String url = String.format(commitPattern, hostPort, committable.getDb());
            HttpPut httpPut = builder.setUrl(url).setEmptyEntity().build();

            // http execute...
            try (CloseableHttpResponse response = httpClient.execute(httpPut)) {
                StatusLine statusLine = response.getStatusLine();
                if (200 == statusLine.getStatusCode()) {
                    if (response.getEntity() != null) {
                        String loadResult = EntityUtils.toString(response.getEntity());
                        Map<String, String> res =
                                jsonMapper.readValue(
                                        loadResult,
                                        new TypeReference<HashMap<String, String>>() {});
                        if (!res.get("status").equals(SUCCESS)
                                && !ResponseUtil.isCommitted(res.get("msg"))) {
                            throw new DorisRuntimeException(
                                    "commit transaction failed " + loadResult);
                        } else {
                            LOG.info("load result {}", loadResult);
                        }
                    }
                    return;
                }
                String reasonPhrase = statusLine.getReasonPhrase();
                LOG.error("commit failed with {}, reason {}", hostPort, reasonPhrase);
                if (retry == maxRetry) {
                    throw new DorisRuntimeException("commit transaction error: " + reasonPhrase);
                }
                hostPort = backendUtil.getAvailableBackend();
            } catch (Exception e) {
                LOG.error("commit transaction failed, to retry, {}", e.getMessage());
                if (retry == maxRetry) {
                    if (CommitTolerance.ALWAYS.equals(commitTolerance)) {
                        LOG.error(
                                "Unable to commit transaction {} and data has been potentially lost ",
                                committable,
                                e);
                    } else if (CommitTolerance.ONCE.equals(commitTolerance)) {
                        // CommitTolerance.ONCE means that the transaction that failed to submit for
                        // the first time (txnid stored in the state) will be ignored when resuming
                        // the task.
                        // Generally used when txn expires and unexpected errors occur in commit.

                        // It should be noted that you must manually ensure that the txn has been
                        // successfully submitted to doris, otherwise there may be a risk of data
                        // loss.
                        committable.setCommitTolerance(CommitTolerance.NEVER);
                        request.updateAndRetryLater(committable);
                        LOG.error("Unable to commit transaction, skip once {} ", e.getMessage());
                    } else {
                        throw new DorisRuntimeException("Commit transaction error, ", e);
                    }
                }
                hostPort = backendUtil.getAvailableBackend();
            }
            retry++;
        }
    }

    @Override
    public void close() {
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
            }
        }
    }
}
