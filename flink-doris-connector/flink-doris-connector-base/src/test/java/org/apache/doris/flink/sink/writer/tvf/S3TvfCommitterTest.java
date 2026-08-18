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

package org.apache.doris.flink.sink.writer.tvf;

import org.apache.flink.api.connector.sink2.Committer.CommitRequest;

import org.apache.doris.flink.cfg.S3TvfOptions;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

public class S3TvfCommitterTest {

    @Test
    public void testCommitsWriterRequestsIndependently() throws Exception {
        RecordingLoadClient loadClient = new RecordingLoadClient();
        S3TvfCommitter committer = createCommitter(loadClient, 3);

        committer.commit(
                Arrays.asList(
                        request(committable("prefix_tbl_0_7_0.json", "label_tbl_0_7")),
                        request(committable("prefix_tbl_1_7_0.json", "label_tbl_1_7"))));

        Assert.assertEquals(2, loadClient.insertCount);
        Assert.assertTrue(loadClient.lastInsertSql.contains("prefix_tbl_1_7_0.json"));
        Assert.assertEquals("true", loadClient.sessionVariables.get("enable_partial_update"));
    }

    @Test
    public void testTransformsStreamLoadPropertiesToSessionVariables() throws Exception {
        RecordingLoadClient loadClient = new RecordingLoadClient();
        Properties properties = new Properties();
        properties.setProperty("columns", "id,name");
        properties.setProperty("partial_columns", "true");
        properties.setProperty("query_timeout", "60");
        S3TvfCommitter committer = new S3TvfCommitter(loadClient, options(), properties, 0);

        committer.commit(Collections.singletonList(request(committable("file.json"))));

        Assert.assertFalse(loadClient.sessionVariables.containsKey("columns"));
        Assert.assertFalse(loadClient.sessionVariables.containsKey("partial_columns"));
        Assert.assertEquals(
                "true", loadClient.sessionVariables.get("enable_unique_key_partial_update"));
        Assert.assertEquals("60", loadClient.sessionVariables.get("query_timeout"));
    }

    @Test
    public void testLabelAlreadyUsedAndFinishedIsSuccessful() throws Exception {
        RecordingLoadClient loadClient = new RecordingLoadClient();
        loadClient.insertFailures.add(labelAlreadyUsed());
        loadClient.states.add(S3TvfLoadState.FINISHED);
        TestCommitRequest request = new TestCommitRequest(committable("file.json"));

        createCommitter(loadClient, 3).commit(Collections.singletonList(request));

        Assert.assertEquals(1, loadClient.insertCount);
        Assert.assertEquals(1, loadClient.showCount);
        Assert.assertEquals(0, loadClient.cancelCount);
        Assert.assertTrue(request.alreadyCommitted);
    }

    @Test
    public void testDifferentLabelDoesNotTriggerReconciliation() throws Exception {
        RecordingLoadClient loadClient = new RecordingLoadClient();
        loadClient.insertFailures.add(
                new SQLException(
                        "errCode = 2, detailMessage = Label [other_label] has already been used."));
        TestCommitRequest request = new TestCommitRequest(committable("file.json"));

        createCommitter(loadClient, 0).commit(Collections.singletonList(request));

        Assert.assertEquals(0, loadClient.showCount);
        Assert.assertFalse(request.alreadyCommitted);
        Assert.assertNotNull(request.failure);
    }

    @Test
    public void testCancelPendingThenRetrySameLabel() throws Exception {
        RecordingLoadClient loadClient = new RecordingLoadClient();
        loadClient.insertFailures.add(labelAlreadyUsed());
        loadClient.states.add(S3TvfLoadState.PENDING);
        loadClient.states.add(S3TvfLoadState.CANCELLED);

        createCommitter(loadClient, 3)
                .commit(Collections.singletonList(request(committable("file.json"))));

        Assert.assertEquals(2, loadClient.insertCount);
        Assert.assertEquals(2, loadClient.showCount);
        Assert.assertEquals(1, loadClient.cancelCount);
    }

    @Test
    public void testUnknownLabelStateDoesNotBlindlyResubmit() throws Exception {
        RecordingLoadClient loadClient = new RecordingLoadClient();
        loadClient.insertFailures.add(labelAlreadyUsed());
        loadClient.states.add(S3TvfLoadState.UNKNOWN);
        loadClient.states.add(S3TvfLoadState.NOT_FOUND);
        loadClient.states.add(S3TvfLoadState.UNKNOWN);
        TestCommitRequest request = new TestCommitRequest(committable("file.json"));

        createCommitter(loadClient, 2).commit(Collections.singletonList(request));

        Assert.assertEquals(1, loadClient.insertCount);
        Assert.assertEquals(3, loadClient.showCount);
        Assert.assertNotNull(request.failure);
        Assert.assertTrue(request.failure.getMessage().contains("label_tbl_7"));
    }

    @Test
    public void testCancelAlwaysRechecksWithZeroRetries() throws Exception {
        RecordingLoadClient loadClient = new RecordingLoadClient();
        loadClient.insertFailures.add(labelAlreadyUsed());
        loadClient.states.add(S3TvfLoadState.PENDING);
        loadClient.states.add(S3TvfLoadState.FINISHED);
        TestCommitRequest request = new TestCommitRequest(committable("file.json"));

        createCommitter(loadClient, 0).commit(Collections.singletonList(request));

        Assert.assertEquals(1, loadClient.insertCount);
        Assert.assertEquals(2, loadClient.showCount);
        Assert.assertEquals(1, loadClient.cancelCount);
        Assert.assertTrue(request.alreadyCommitted);
    }

    @Test
    public void testCancelRaceRechecksFinishedState() throws Exception {
        RecordingLoadClient loadClient = new RecordingLoadClient();
        loadClient.insertFailures.add(labelAlreadyUsed());
        loadClient.states.add(S3TvfLoadState.LOADING);
        loadClient.states.add(S3TvfLoadState.FINISHED);
        loadClient.cancelFailures.add(new SQLException("load already finished"));
        TestCommitRequest request = new TestCommitRequest(committable("file.json"));

        createCommitter(loadClient, 0).commit(Collections.singletonList(request));

        Assert.assertEquals(2, loadClient.showCount);
        Assert.assertEquals(1, loadClient.cancelCount);
        Assert.assertTrue(request.alreadyCommitted);
    }

    @Test
    public void testInsertFailureDoesNotExposeSqlException() throws Exception {
        RecordingLoadClient loadClient = new RecordingLoadClient();
        loadClient.insertFailures.add(new SQLException("failed SQL contains secret-sk"));
        TestCommitRequest request = new TestCommitRequest(committable("file.json"));

        createCommitter(loadClient, 0).commit(Collections.singletonList(request));

        Assert.assertNotNull(request.failure);
        Assert.assertFalse(request.failure.toString().contains("secret-sk"));
        Assert.assertNull(request.failure.getCause());
    }

    private static S3TvfCommitter createCommitter(RecordingLoadClient loadClient, int maxRetries) {
        Properties sessionVariables = new Properties();
        sessionVariables.setProperty("enable_partial_update", "true");
        return new S3TvfCommitter(loadClient, options(), sessionVariables, maxRetries);
    }

    private static S3TvfOptions options() {
        return S3TvfOptions.builder()
                .setEndpoint("https://s3.example.com")
                .setRegion("us-east-1")
                .setBucket("bucket")
                .setPrefix("prefix")
                .setAccessKey("ak")
                .setSecretKey("sk")
                .build();
    }

    private static S3TvfCommittable committable(String objectKey) {
        return committable(objectKey, "label_tbl_7");
    }

    private static S3TvfCommittable committable(String objectKey, String label) {
        return new S3TvfCommittable(
                7L,
                "db",
                "tbl",
                label,
                Collections.singletonList(objectKey),
                Collections.singletonList("id"),
                false);
    }

    private static CommitRequest<S3TvfCommittable> request(S3TvfCommittable committable) {
        return new TestCommitRequest(committable);
    }

    private static SQLException labelAlreadyUsed() {
        return new SQLException(
                "errCode = 2, detailMessage = Label [label_tbl_7] has already been used");
    }

    private static class RecordingLoadClient implements S3TvfLoadClient {
        private final Queue<SQLException> insertFailures = new ArrayDeque<>();
        private final Queue<SQLException> cancelFailures = new ArrayDeque<>();
        private final Queue<S3TvfLoadState> states = new ArrayDeque<>();
        private int insertCount;
        private int showCount;
        private int cancelCount;
        private String lastInsertSql;
        private Map<String, String> sessionVariables;

        @Override
        public void executeInsert(String sql, Map<String, String> sessionVariables)
                throws SQLException {
            this.sessionVariables = sessionVariables;
            insertCount++;
            lastInsertSql = sql;
            if (!insertFailures.isEmpty()) {
                throw insertFailures.remove();
            }
        }

        @Override
        public S3TvfLoadState getLoadState(String database, String label) {
            showCount++;
            return states.isEmpty() ? S3TvfLoadState.UNKNOWN : states.remove();
        }

        @Override
        public void cancelLoad(String database, String label) throws SQLException {
            cancelCount++;
            if (!cancelFailures.isEmpty()) {
                throw cancelFailures.remove();
            }
        }

        @Override
        public void close() {}
    }

    private static class TestCommitRequest implements CommitRequest<S3TvfCommittable> {
        private final S3TvfCommittable committable;
        private Throwable failure;
        private boolean alreadyCommitted;

        private TestCommitRequest(S3TvfCommittable committable) {
            this.committable = committable;
        }

        @Override
        public S3TvfCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {}

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            failure = t;
        }

        @Override
        public void retryLater() {}

        @Override
        public void updateAndRetryLater(S3TvfCommittable committable) {}

        @Override
        public void signalAlreadyCommitted() {
            alreadyCommitted = true;
        }
    }
}
