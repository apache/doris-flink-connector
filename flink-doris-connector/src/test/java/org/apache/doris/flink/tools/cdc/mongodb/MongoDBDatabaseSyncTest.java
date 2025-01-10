package org.apache.doris.flink.tools.cdc.mongodb;

import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class MongoDBDatabaseSyncTest {
    private MongoDBDatabaseSync mongoDBDatabaseSync;

    @Before
    public void init() throws SQLException {
        mongoDBDatabaseSync = new MongoDBDatabaseSync();
    }

    @Test
    public void testCalculateSampleSize() {
        long sampleSize1 = mongoDBDatabaseSync.calculateSampleSize(100L, 0.2, 1000L, 100000L);
        long sampleSize2 = mongoDBDatabaseSync.calculateSampleSize(1000L, 0.2, 1000L, 100000L);
        long sampleSize3 = mongoDBDatabaseSync.calculateSampleSize(2000L, 0.2, 1000L, 100000L);
        long sampleSize4 = mongoDBDatabaseSync.calculateSampleSize(10000L, 0.2, 1000L, 100000L);
        long sampleSize5 = mongoDBDatabaseSync.calculateSampleSize(1000000L, 0.2, 1000L, 100000L);
        assertEquals(100, sampleSize1);
        assertEquals(1000, sampleSize2);
        assertEquals(1000, sampleSize3);
        assertEquals(2000, sampleSize4);
        assertEquals(100000, sampleSize5);
    }
}
