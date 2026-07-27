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

package org.apache.doris.flink.source;

import org.apache.flink.types.RowKind;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/** Integration cases for Doris incremental Source startup modes and changelog records. */
public class DorisIncrementalSourceITCase extends AbstractDorisIncrementalITCase {
    private static final String TABLE_INITIAL = "tbl_incremental_initial";
    private static final String TABLE_LATEST = "tbl_incremental_latest";
    private static final String TABLE_FROM_TIMESTAMP = "tbl_incremental_from_timestamp";

    @Test
    public void testInitialDetailChangelogAcrossSnapshotBoundary() throws Exception {
        initializeIncrementalTable(
                TABLE_INITIAL,
                "(1,'snapshot-1','ignored-1')",
                "(2,'snapshot-2','ignored-2')",
                "(3,'snapshot-3','ignored-3')");

        try (IncrementalResultCollector collector = startSource(TABLE_INITIAL, "initial", null)) {
            ObservedRow firstSnapshotRow =
                    collector.awaitRow(
                            row ->
                                    row.getRowKind() == RowKind.INSERT
                                            && row.getInt(0) >= 1
                                            && row.getInt(0) <= 3,
                            "No initial Snapshot row was emitted");
            int changedId = firstSnapshotRow.getInt(0);
            String originalName = firstSnapshotRow.getString(1);
            String updatedName = "updated-" + changedId;

            executeDorisSql(
                    String.format(
                            "UPDATE %s.%s SET name = '%s' WHERE id = %d",
                            DATABASE, TABLE_INITIAL, updatedName, changedId),
                    String.format(
                            "DELETE FROM %s.%s WHERE id = %d", DATABASE, TABLE_INITIAL, changedId),
                    String.format(
                            "INSERT INTO %s.%s VALUES (100,'inserted','ignored-100')",
                            DATABASE, TABLE_INITIAL));

            List<ObservedRow> snapshotRows =
                    Arrays.asList(
                            ObservedRow.of(RowKind.INSERT, 1, "snapshot-1"),
                            ObservedRow.of(RowKind.INSERT, 2, "snapshot-2"),
                            ObservedRow.of(RowKind.INSERT, 3, "snapshot-3"));
            collector.awaitContainsAll(snapshotRows);
            completeCheckpoint(collector.getJobClient());

            ObservedRow updateBefore =
                    ObservedRow.of(RowKind.UPDATE_BEFORE, changedId, originalName);
            ObservedRow updateAfter = ObservedRow.of(RowKind.UPDATE_AFTER, changedId, updatedName);
            ObservedRow delete = ObservedRow.of(RowKind.DELETE, changedId, updatedName);
            ObservedRow insert = ObservedRow.of(RowKind.INSERT, 100, "inserted");
            collector.awaitContainsAll(Arrays.asList(updateBefore, updateAfter, delete, insert));

            List<ObservedRow> observedRows = collector.getRows();
            for (ObservedRow row : observedRows) {
                Assert.assertEquals(
                        "Hidden binlog fields escaped the projection", 2, row.getArity());
            }
            Assert.assertTrue(
                    "UPDATE_BEFORE must precede UPDATE_AFTER",
                    observedRows.indexOf(updateBefore) < observedRows.indexOf(updateAfter));
            Assert.assertTrue(
                    "UPDATE_AFTER must precede DELETE",
                    observedRows.indexOf(updateAfter) < observedRows.indexOf(delete));
        }
    }

    @Test
    public void testLatestReadsChangesAfterStartupWithoutSnapshot() throws Exception {
        initializeIncrementalTable(TABLE_LATEST, "(1,'old-row','ignored-old')");
        String oldRowTimestamp = resolveCurrentDorisTimestamp(TABLE_LATEST);
        waitForDorisTimestampAfter(TABLE_LATEST, oldRowTimestamp);

        try (IncrementalResultCollector collector = startSource(TABLE_LATEST, "latest", null)) {
            completeCheckpoint(collector.getJobClient());
            executeDorisSql(
                    String.format(
                            "INSERT INTO %s.%s VALUES (2,'new-row','ignored-new')",
                            DATABASE, TABLE_LATEST));

            collector.awaitContains(ObservedRow.of(RowKind.INSERT, 2, "new-row"));
            Assert.assertFalse(
                    "latest mode unexpectedly emitted the old Snapshot row",
                    collector.getRows().stream().anyMatch(row -> row.getInt(0) == 1));
        }
    }

    @Test
    public void testFromTimestampReplaysHistoricalChanges() throws Exception {
        initializeIncrementalTable(TABLE_FROM_TIMESTAMP);
        String startTimestamp = resolveCurrentDorisTimestamp(TABLE_FROM_TIMESTAMP);
        executeDorisSql(
                String.format(
                        "INSERT INTO %s.%s VALUES (7,'historical-row','ignored-history')",
                        DATABASE, TABLE_FROM_TIMESTAMP));
        waitForDorisTimestampAfter(TABLE_FROM_TIMESTAMP, startTimestamp);

        try (IncrementalResultCollector collector =
                startSource(TABLE_FROM_TIMESTAMP, "from-timestamp", startTimestamp)) {
            ObservedRow historicalRow = ObservedRow.of(RowKind.INSERT, 7, "historical-row");
            collector.awaitContains(historicalRow);
            Assert.assertEquals(
                    "from-timestamp mode emitted unexpected Snapshot rows",
                    Arrays.asList(historicalRow),
                    collector.getRows());
        }
    }
}
