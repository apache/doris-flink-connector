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

package org.apache.doris.flink.sink.overwrite;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.writer.WriteMode;
import org.junit.Assert;
import org.junit.Test;

/** Tests for Doris overwrite staging helpers. */
public class TestDorisOverwriteManager {

    @Test
    public void testTableIdentifier() {
        DorisTableIdentifier identifier = DorisTableIdentifier.of("db.tbl");
        Assert.assertEquals("db", identifier.getDatabase());
        Assert.assertEquals("tbl", identifier.getTable());
        Assert.assertEquals("db.tbl", identifier.asString());
        Assert.assertEquals("`db`.`tbl`", identifier.toSql());
    }

    @Test
    public void testInvalidTableIdentifier() {
        assertInvalidIdentifier(null);
        assertInvalidIdentifier("");
        assertInvalidIdentifier("tbl");
        assertInvalidIdentifier("catalog.db.tbl");
        assertInvalidIdentifier("db.");
        assertInvalidIdentifier(".tbl");
    }

    @Test
    public void testStagingTableNameIsStableAndBounded() {
        DorisTableIdentifier target = DorisTableIdentifier.of("db.tbl");
        String staging = DorisOverwriteManager.buildStagingTableName(target, "label-prefix");
        Assert.assertEquals(
                staging, DorisOverwriteManager.buildStagingTableName(target, "label-prefix"));
        Assert.assertTrue(staging.startsWith("__doris_flink_overwrite_label_prefix_"));
        Assert.assertTrue(staging.length() <= 63);
    }

    @Test
    public void testSqlGeneration() {
        DorisTableIdentifier target = DorisTableIdentifier.of("db.tbl");
        DorisTableIdentifier staging = DorisTableIdentifier.of("db.__staging");
        DorisOverwriteOptions options =
                new DorisOverwriteOptions(
                        DorisOptions.builder()
                                .setFenodes("127.0.0.1:8030")
                                .setJdbcUrl("jdbc:mysql://127.0.0.1:9030")
                                .setTableIdentifier(target.asString())
                                .build(),
                        target,
                        staging,
                        10L,
                        20L,
                        "label");
        Assert.assertEquals(
                "CREATE TABLE `db`.`__staging` LIKE `db`.`tbl`",
                DorisOverwriteManager.createTableLikeSql(staging, target));
        Assert.assertEquals(
                "ALTER TABLE `db`.`tbl` REPLACE WITH TABLE `__staging` PROPERTIES('swap'='false')",
                DorisOverwriteManager.replaceTableSql(options));
    }

    @Test
    public void testCopyOptions() {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("fe:8030")
                        .setBenodes("be:8040")
                        .setUsername("root")
                        .setPassword("pwd")
                        .setJdbcUrl("jdbc:mysql://fe:9030")
                        .setAutoRedirect(false)
                        .setTableIdentifier("db.tbl")
                        .build();
        DorisOptions copied = DorisOverwriteManager.copyOptions(options, "db.staging");
        Assert.assertEquals("db.staging", copied.getTableIdentifier());
        Assert.assertEquals(options.getFenodes(), copied.getFenodes());
        Assert.assertEquals(options.getBenodes(), copied.getBenodes());
        Assert.assertEquals(options.getUsername(), copied.getUsername());
        Assert.assertEquals(options.getPassword(), copied.getPassword());
        Assert.assertEquals(options.getJdbcUrl(), copied.getJdbcUrl());
        Assert.assertEquals(options.isAutoRedirect(), copied.isAutoRedirect());
    }

    @Test
    public void testValidateOverwriteOptions() {
        DorisOverwriteManager.validateOverwriteOptions(
                DorisOptions.builder()
                        .setFenodes("fe:8030")
                        .setJdbcUrl("jdbc:mysql://fe:9030")
                        .setTableIdentifier("db.tbl")
                        .build(),
                DorisExecutionOptions.builder().setLabelPrefix("label").build());
    }

    @Test
    public void testValidateOverwriteOptionsRejectsIgnoreCommitError() {
        assertInvalidOverwriteOptions(
                DorisOptions.builder()
                        .setFenodes("fe:8030")
                        .setJdbcUrl("jdbc:mysql://fe:9030")
                        .setTableIdentifier("db.tbl")
                        .build(),
                DorisExecutionOptions.builder()
                        .setLabelPrefix("label")
                        .setIgnoreCommitError(true)
                        .build(),
                "sink.ignore.commit-error");
    }

    @Test
    public void testValidateOverwriteOptionsRejectsUnsafeModes() {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("fe:8030")
                        .setJdbcUrl("jdbc:mysql://fe:9030")
                        .setTableIdentifier("db.tbl")
                        .build();
        assertInvalidOverwriteOptions(
                options,
                DorisExecutionOptions.builder().disable2PC().setLabelPrefix("label").build(),
                "sink.enable-2pc");
        assertInvalidOverwriteOptions(
                options,
                DorisExecutionOptions.builder()
                        .setLabelPrefix("label")
                        .setWriteMode(WriteMode.COPY)
                        .build(),
                "STREAM_LOAD");
        assertInvalidOverwriteOptions(
                options, DorisExecutionOptions.builder().build(), "sink.label-prefix");
    }

    private void assertInvalidIdentifier(String tableIdentifier) {
        try {
            DorisTableIdentifier.of(tableIdentifier);
            Assert.fail("Expected invalid identifier: " + tableIdentifier);
        } catch (IllegalArgumentException expected) {
        }
    }

    private void assertInvalidOverwriteOptions(
            DorisOptions options, DorisExecutionOptions executionOptions, String messagePart) {
        try {
            DorisOverwriteManager.validateOverwriteOptions(options, executionOptions);
            Assert.fail("Expected invalid overwrite options.");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains(messagePart));
        }
    }
}
