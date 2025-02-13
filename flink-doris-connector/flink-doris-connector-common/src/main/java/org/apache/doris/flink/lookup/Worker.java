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

package org.apache.doris.flink.lookup;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.types.DataType;

import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.connection.JdbcConnectionProvider;
import org.apache.doris.flink.connection.SimpleJdbcConnectionProvider;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class Worker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private final String name;
    private final AtomicBoolean started;
    private final JdbcConnectionProvider jdbcConnectionProvider;
    private ArrayBlockingQueue<GetAction> queue = new ArrayBlockingQueue(1);
    private final int maxRetryTimes;
    private AtomicReference<Throwable> exception = new AtomicReference<>(null);

    public Worker(
            AtomicBoolean started,
            DorisOptions options,
            DorisLookupOptions lookupOptions,
            int index) {
        this.started = started;
        this.name = "Worker-" + index;
        this.jdbcConnectionProvider = new SimpleJdbcConnectionProvider(options);
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
    }

    public boolean offer(GetAction action) {
        if (exception.get() != null) {
            throw new DorisRuntimeException(exception.get());
        }
        return queue.offer(action);
    }

    @Override
    public void run() {
        LOG.info("worker:{} start", this);
        while (started.get()) {
            try {
                GetAction action = queue.poll(2000L, TimeUnit.MILLISECONDS);
                if (action != null) {
                    try {
                        handle(action);
                    } finally {
                        if (action.getSemaphore() != null) {
                            action.getSemaphore().release();
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("worker running error", e);
                exception.set(e);
                break;
            }
        }
        LOG.info("worker:{} stop", this);
        jdbcConnectionProvider.closeConnection();
    }

    private void handle(GetAction action) {
        if (action.getGetList().size() <= 0) {
            return;
        }
        LookupSchema schema = action.getGetList().get(0).getRecord().getSchema();
        List<Get> recordList = action.getGetList();
        List<Get> deduplicateList = deduplicateRecords(recordList);
        LOG.debug(
                "record size {}, after deduplicate size {}",
                recordList.size(),
                deduplicateList.size());
        StringBuilder sb = new StringBuilder();
        boolean first;
        for (int i = 0; i < deduplicateList.size(); i++) {
            if (i > 0) {
                sb.append(" union all ");
            }
            first = true;
            appendSelect(sb, schema);
            sb.append(" where ( ");
            for (String condition : schema.getConditionFields()) {
                if (!first) {
                    sb.append(" and ");
                }
                first = false;
                sb.append(quoteIdentifier(condition)).append("=?");
            }
            sb.append(" ) ");
        }

        String sql = sb.toString();
        try {
            Map<RecordKey, List<Record>> resultRecordMap =
                    executeQuery(sql, deduplicateList, schema);
            for (Get get : recordList) {
                Record record = get.getRecord();
                if (get.getFuture() != null) {
                    RecordKey key = new RecordKey(record);
                    List<Record> result = resultRecordMap.get(key);
                    get.getFuture().complete(result);
                }
            }
        } catch (Exception e) {
            for (Get get : recordList) {
                if (get.getFuture() != null && !get.getFuture().isDone()) {
                    get.getFuture().completeExceptionally(e);
                }
            }
        }
    }

    /**
     * Sometimes, there will be duplicate key filtering conditions in a batch of data, which can be
     * removed in advance to reduce query pressure.
     */
    @VisibleForTesting
    public static List<Get> deduplicateRecords(List<Get> recordList) {
        if (recordList == null || recordList.size() <= 1) {
            return recordList;
        }
        Set<Get> recordSet =
                new TreeSet<>(
                        (r1, r2) ->
                                Arrays.equals(
                                                r1.getRecord().getValues(),
                                                r2.getRecord().getValues())
                                        ? 0
                                        : -1);
        recordSet.addAll(recordList);
        return new ArrayList<>(recordSet);
    }

    private void appendSelect(StringBuilder sb, LookupSchema schema) {
        String[] selectFields = schema.getSelectFields();
        sb.append("/* ApplicationName=Flink Lookup Query */ ");
        sb.append(" select ");
        for (int i = 0; i < selectFields.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            String columnName = selectFields[i];
            sb.append(quoteIdentifier(columnName));
        }
        sb.append(" from ").append(schema.getTableIdentifier());
    }

    private Map<RecordKey, List<Record>> executeQuery(
            String sql, List<Get> recordList, LookupSchema schema) {
        Map<RecordKey, List<Record>> resultRecordMap = new HashMap<>();
        // retry strategy
        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            resultRecordMap = new HashMap<>();
            try {
                long start = System.currentTimeMillis();
                Connection conn = jdbcConnectionProvider.getOrEstablishConnection();
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    int paramIndex = 0;
                    for (Get get : recordList) {
                        Record record = get.getRecord();
                        for (int keyIndex : schema.getKeyIndex()) {
                            ps.setObject(++paramIndex, record.getObject(keyIndex));
                        }
                    }

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            Record record = new Record(schema);
                            DataType[] fieldTypes = schema.getFieldTypes();
                            for (int index = 0; index < fieldTypes.length; index++) {
                                Class<?> conversionClass = fieldTypes[index].getConversionClass();
                                record.setObject(index, rs.getObject(index + 1, conversionClass));
                            }
                            List<Record> records =
                                    resultRecordMap.computeIfAbsent(
                                            new RecordKey(record), m -> new ArrayList<>());
                            records.add(record);
                        }
                    }
                }
                LOG.debug(
                        "query cost {}ms, batch {} records, sql is {}",
                        System.currentTimeMillis() - start,
                        recordList.size(),
                        sql);
                return resultRecordMap;
            } catch (Exception e) {
                LOG.error(String.format("query doris error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException(e);
                }
                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        return resultRecordMap;
    }

    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    public String toString() {
        return name;
    }
}
