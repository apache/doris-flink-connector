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

package org.apache.doris.flink.sink.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.exception.StreamLoadException;
import org.apache.doris.flink.rest.models.RespContent;
import org.apache.doris.flink.sink.EscapeHandler;
import org.apache.doris.flink.sink.HttpPutBuilder;
import org.apache.doris.flink.sink.ResponseUtil;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.NoRouteToHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static org.apache.doris.flink.sink.LoadStatus.LABEL_ALREADY_EXIST;
import static org.apache.doris.flink.sink.LoadStatus.SUCCESS;
import static org.apache.doris.flink.sink.ResponseUtil.LABEL_EXIST_PATTERN;
import static org.apache.doris.flink.sink.writer.LoadConstants.ARROW;
import static org.apache.doris.flink.sink.writer.LoadConstants.COMPRESS_TYPE;
import static org.apache.doris.flink.sink.writer.LoadConstants.COMPRESS_TYPE_GZ;
import static org.apache.doris.flink.sink.writer.LoadConstants.CSV;
import static org.apache.doris.flink.sink.writer.LoadConstants.FORMAT_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.GROUP_COMMIT;
import static org.apache.doris.flink.sink.writer.LoadConstants.GROUP_COMMIT_OFF_MODE;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_DEFAULT;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

/** load data to doris. */
public class DorisStreamLoad implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoad.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final LabelGenerator labelGenerator;
    private final byte[] lineDelimiter;
    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private static final String ABORT_URL_PATTERN = "http://%s/api/%s/_stream_load_2pc";
    public static final String JOB_EXIST_FINISHED = "FINISHED";

    private String loadUrlStr;
    private String hostPort;
    private String abortUrlStr;
    private final String user;
    private final String passwd;
    private final String db;
    private final String table;
    private final boolean enable2PC;
    private final boolean enableDelete;
    private final Properties streamLoadProp;
    private final RecordStream recordStream;
    private volatile Future<CloseableHttpResponse> pendingLoadFuture;
    private final CloseableHttpClient httpClient;
    private final ExecutorService executorService;
    private boolean loadBatchFirstRecord;
    private volatile String currentLabel;
    private boolean enableGroupCommit;
    private boolean enableGzCompress;

    public DorisStreamLoad(
            String hostPort,
            DorisOptions dorisOptions,
            DorisExecutionOptions executionOptions,
            LabelGenerator labelGenerator,
            CloseableHttpClient httpClient) {
        this.hostPort = hostPort;
        String[] tableInfo = dorisOptions.getTableIdentifier().split("\\.");
        this.db = tableInfo[0];
        this.table = tableInfo[1];
        this.user = dorisOptions.getUsername();
        this.passwd = dorisOptions.getPassword();
        this.labelGenerator = labelGenerator;
        this.loadUrlStr = String.format(LOAD_URL_PATTERN, hostPort, db, table);
        this.abortUrlStr = String.format(ABORT_URL_PATTERN, hostPort, db);
        this.enable2PC = executionOptions.enabled2PC();
        this.streamLoadProp = executionOptions.getStreamLoadProp();
        this.enableDelete = executionOptions.getDeletable();
        this.httpClient = httpClient;
        this.executorService =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        new ExecutorThreadFactory("stream-load-upload"));
        this.recordStream =
                new RecordStream(
                        executionOptions.getBufferSize(),
                        executionOptions.getBufferCount(),
                        executionOptions.isUseCache());
        if (streamLoadProp.getProperty(FORMAT_KEY, CSV).equals(ARROW)) {
            lineDelimiter = null;
        } else {
            lineDelimiter =
                    EscapeHandler.escapeString(
                                    streamLoadProp.getProperty(
                                            LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT))
                            .getBytes();
        }
        this.enableGroupCommit =
                streamLoadProp.containsKey(GROUP_COMMIT)
                        && !streamLoadProp
                                .getProperty(GROUP_COMMIT)
                                .equalsIgnoreCase(GROUP_COMMIT_OFF_MODE);
        this.enableGzCompress =
                streamLoadProp.getProperty(COMPRESS_TYPE, "").equals(COMPRESS_TYPE_GZ);
        loadBatchFirstRecord = true;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public String getHostPort() {
        return hostPort;
    }

    @VisibleForTesting
    public byte[] getLineDelimiter() {
        return lineDelimiter;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
        this.loadUrlStr = String.format(LOAD_URL_PATTERN, hostPort, this.db, this.table);
        this.abortUrlStr = String.format(ABORT_URL_PATTERN, hostPort, db);
    }

    public Future<CloseableHttpResponse> getPendingLoadFuture() {
        return pendingLoadFuture;
    }

    /**
     * try to discard pending transactions with labels beginning with labelPrefix.
     *
     * @param labelPrefix the prefix of the stream load.
     * @param chkID checkpoint id of task.
     * @throws Exception
     */
    public void abortPreCommit(String labelPrefix, long chkID) throws Exception {
        long startChkID = chkID;
        LOG.info(
                "abort for labelPrefix {}, concat labelPrefix {},  start chkId {}.",
                labelPrefix,
                labelGenerator.getConcatLabelPrefix(),
                chkID);
        while (true) {
            try {
                // TODO: According to label abort txn.
                //  Currently, it can only be aborted based on txnid, so we must
                //  first request a streamload based on the label to get the txnid.
                String label = labelGenerator.generateTableLabel(startChkID);
                LOG.info("start a check label {} to load.", label);
                HttpPutBuilder builder = new HttpPutBuilder();
                builder.setUrl(loadUrlStr)
                        .baseAuth(user, passwd)
                        .addCommonHeader()
                        .enable2PC()
                        .setLabel(label)
                        .setEmptyEntity()
                        .addProperties(streamLoadProp);
                RespContent respContent =
                        handlePreCommitResponse(httpClient.execute(builder.build()));
                Preconditions.checkState("true".equals(respContent.getTwoPhaseCommit()));
                if (LABEL_ALREADY_EXIST.equals(respContent.getStatus())) {
                    // label already exist and job finished
                    if (JOB_EXIST_FINISHED.equals(respContent.getExistingJobStatus())) {
                        throw new DorisException(
                                "Load status is "
                                        + LABEL_ALREADY_EXIST
                                        + " and load job finished, "
                                        + "change you label prefix or restore from latest savepoint!");
                    }
                    // job not finished, abort.
                    Matcher matcher = LABEL_EXIST_PATTERN.matcher(respContent.getMessage());
                    if (matcher.find()) {
                        Preconditions.checkState(label.equals(matcher.group(1)));
                        long txnId = Long.parseLong(matcher.group(2));
                        LOG.info("abort {} for exist label {}", txnId, label);
                        abortTransaction(txnId);
                    } else {
                        LOG.error("response: {}", respContent.toString());
                        throw new DorisException(
                                "Load Status is "
                                        + LABEL_ALREADY_EXIST
                                        + ", but no txnID associated with it!");
                    }
                } else {
                    LOG.info("abort {} for check label {}.", respContent.getTxnId(), label);
                    abortTransaction(respContent.getTxnId());
                    break;
                }
                startChkID++;
            } catch (Exception e) {
                LOG.warn("failed to abort labelPrefix {}", labelPrefix, e);
                throw e;
            }
        }
        LOG.info("abort for labelPrefix {} finished", labelPrefix);
    }

    /**
     * write record into stream.
     *
     * @param record
     * @throws IOException
     */
    public void writeRecord(byte[] record) throws IOException {
        if (loadBatchFirstRecord) {
            loadBatchFirstRecord = false;
        } else if (lineDelimiter != null) {
            recordStream.write(lineDelimiter);
        }
        recordStream.write(record);
    }

    @VisibleForTesting
    public RecordStream getRecordStream() {
        return recordStream;
    }

    public RespContent handlePreCommitResponse(CloseableHttpResponse response) throws Exception {
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200 && response.getEntity() != null) {
            String loadResult = EntityUtils.toString(response.getEntity());
            LOG.info("load Result {}", loadResult);
            return OBJECT_MAPPER.readValue(loadResult, RespContent.class);
        }
        throw new StreamLoadException("stream load error: " + response.getStatusLine().toString());
    }

    public RespContent stopLoad() throws IOException {
        recordStream.endInput();
        if (enableGroupCommit) {
            LOG.info("table {} stream load stopped with group commit on host {}", table, hostPort);
        } else {
            LOG.info(
                    "table {} stream load stopped for {} on host {}",
                    table,
                    currentLabel,
                    hostPort);
        }

        Preconditions.checkState(pendingLoadFuture != null);
        try {
            return handlePreCommitResponse(pendingLoadFuture.get());
        } catch (NoRouteToHostException nex) {
            LOG.error("Failed to connect, cause ", nex);
            throw new DorisRuntimeException(
                    "No Route to Host to " + hostPort + ", exception: " + nex);
        } catch (Exception e) {
            throw new DorisRuntimeException(e);
        }
    }

    /**
     * start write data for new checkpoint.
     *
     * @param label the label of Stream Load.
     * @throws IOException
     */
    public void startLoad(String label, boolean isResume) throws IOException {
        if (enableGroupCommit) {
            label = null;
        }
        loadBatchFirstRecord = !isResume;
        HttpPutBuilder putBuilder = new HttpPutBuilder();
        recordStream.startInput(isResume);
        if (enableGroupCommit) {
            LOG.info("table {} stream load started with group commit on host {}", table, hostPort);
        } else {
            LOG.info("table {} stream load started for {} on host {}", table, label, hostPort);
        }
        this.currentLabel = label;
        try {
            InputStreamEntity entity = new InputStreamEntity(recordStream);
            putBuilder
                    .setUrl(loadUrlStr)
                    .baseAuth(user, passwd)
                    .addCommonHeader()
                    .addHiddenColumns(enableDelete)
                    .setLabel(label)
                    .setEntity(entity)
                    .addProperties(streamLoadProp);
            if (enable2PC) {
                putBuilder.enable2PC();
            }

            if (enableGzCompress) {
                putBuilder.setEntity(new GzipCompressingEntity(entity));
            }

            String executeMessage;
            if (enableGroupCommit) {
                executeMessage = "table " + table + " start execute load with group commit";
            } else {
                executeMessage = "table " + table + " start execute load for label " + label;
            }
            pendingLoadFuture =
                    executorService.submit(
                            () -> {
                                LOG.info(executeMessage);
                                return httpClient.execute(putBuilder.build());
                            });
        } catch (Exception e) {
            String err;
            if (enableGroupCommit) {
                err = "failed to stream load data with group commit";
            } else {
                err = "failed to stream load data with label: " + label;
            }
            LOG.warn(err, e);
            throw e;
        }
    }

    public void abortTransaction(long txnID) throws Exception {
        HttpPutBuilder builder = new HttpPutBuilder();
        builder.setUrl(abortUrlStr)
                .baseAuth(user, passwd)
                .addCommonHeader()
                .addTxnId(txnID)
                .setEmptyEntity()
                .abort();
        CloseableHttpResponse response = httpClient.execute(builder.build());

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200 || response.getEntity() == null) {
            LOG.warn("abort transaction response: " + response.getStatusLine().toString());
            throw new DorisRuntimeException(
                    "Fail to abort transaction " + txnID + " with url " + abortUrlStr);
        }

        ObjectMapper mapper = new ObjectMapper();
        String loadResult = EntityUtils.toString(response.getEntity());
        LOG.info("abort Result {}", loadResult);
        Map<String, String> res =
                mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {});
        if (!SUCCESS.equals(res.get("status"))) {
            String msg = res.get("msg");
            // transaction already aborted
            if (msg != null && ResponseUtil.isAborted(msg)) {
                LOG.warn("Failed to abort transaction, {}", msg);
                return;
            }

            LOG.error("Fail to abort transaction. txnId: {}, error: {}", txnID, msg);
            throw new DorisException("Fail to abort transaction, " + loadResult);
        }
    }

    public void abortTransactionByLabel(String label) throws Exception {
        if (StringUtils.isNullOrWhitespaceOnly(label)) {
            return;
        }
        HttpPutBuilder builder = new HttpPutBuilder();
        builder.setUrl(abortUrlStr)
                .baseAuth(user, passwd)
                .addCommonHeader()
                .setLabel(label)
                .setEmptyEntity()
                .abort();
        CloseableHttpResponse response = httpClient.execute(builder.build());

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200 || response.getEntity() == null) {
            LOG.warn("abort transaction by label response: " + response.getStatusLine().toString());
            throw new DorisRuntimeException(
                    "Fail to abort transaction by label " + label + " with url " + abortUrlStr);
        }

        ObjectMapper mapper = new ObjectMapper();
        String loadResult = EntityUtils.toString(response.getEntity());
        LOG.info("abort Result {}", loadResult);
        Map<String, String> res =
                mapper.readValue(loadResult, new TypeReference<HashMap<String, String>>() {});
        if (!SUCCESS.equals(res.get("status"))) {
            String msg = res.get("msg");
            if (msg != null && ResponseUtil.isCommitted(msg)) {
                throw new DorisException(
                        "try abort committed transaction by label, "
                                + "do you recover from old savepoint?");
            }

            LOG.error("Fail to abort transaction by label. label: {}, error: {}", label, msg);
            throw new DorisException("Fail to abort transaction by label, " + loadResult);
        }
    }

    public void abortLabelExistTransaction(RespContent respContent) {
        if (respContent == null || respContent.getMessage() == null) {
            return;
        }
        try {
            Matcher matcher = LABEL_EXIST_PATTERN.matcher(respContent.getMessage());
            if (matcher.find()) {
                long txnId = Long.parseLong(matcher.group(2));
                abortTransaction(txnId);
                LOG.info(
                        "Finish to abort transaction {} for label already exist {}",
                        txnId,
                        respContent.getLabel());
            }
        } catch (Exception ex) {
            LOG.error(
                    "Failed abort transaction {} for label already exist", respContent.getLabel());
        }
    }

    public String getCurrentLabel() {
        return currentLabel;
    }

    public void close() throws IOException {
        if (null != httpClient) {
            try {
                httpClient.close();
            } catch (IOException e) {
                throw new IOException("Closing httpClient failed.", e);
            }
        }
        if (null != executorService) {
            executorService.shutdownNow();
        }
    }
}
