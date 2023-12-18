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

package org.apache.doris.flink.table;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.converter.DorisRowConverter;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.source.reader.DorisValueReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

public class DorisRowDataLookupFunction extends TableFunction<RowData> {
    private static final Logger logger = LoggerFactory.getLogger(DorisRowDataLookupFunction.class);

    private final DorisOptions options;
    private final DorisReadOptions readOptions;
    private final String[] selectFields;
    private final String[] conditionFields;

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;

    private final DorisRowConverter rowConverter;
    private transient Cache<RowData, List<RowData>> cache;

    public DorisRowDataLookupFunction(
            DorisOptions options,
            DorisReadOptions readOptions,
            DorisLookupOptions lookupOptions,
            String[] selectFields,
            DataType[] fieldTypes,
            String[] conditionFields) {
        this.options = options;
        this.readOptions = readOptions;
        this.selectFields = selectFields;
        this.conditionFields = conditionFields;
        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
        this.rowConverter = new DorisRowConverter(fieldTypes);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.cache =
                cacheMaxSize == -1 || cacheExpireMs == -1
                        ? null
                        : CacheBuilder.newBuilder()
                                .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                                .maximumSize(cacheMaxSize)
                                .build();
    }

    /**
     * This is a lookup method which is called by Flink framework in runtime.
     *
     * @param keys lookup keys
     */
    public void eval(Object... keys) {
        RowData keyRow = GenericRowData.of(keys);
        if (cache != null) {
            List<RowData> cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {
                for (RowData cachedRow : cachedRows) {
                    collect(cachedRow);
                }
                return;
            }
        }

        List<PartitionDefinition> partitions = getPartitions(keys);
        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                ArrayList<RowData> rows = new ArrayList<>();
                for (PartitionDefinition part : partitions) {
                    try (DorisValueReader valueReader =
                            new DorisValueReader(part, options, readOptions)) {
                        while (valueReader.hasNext()) {
                            List<?> record = valueReader.next();
                            GenericRowData rowData = rowConverter.convertInternal(record);
                            rows.add(rowData);
                            collect(rowData);
                        }
                    }
                }
                if (cache != null) {
                    rows.trimToSize();
                    cache.put(keyRow, rows);
                }
                break;
            } catch (Exception ex) {
                logger.error(String.format("Read Doris error, retry times = %d", retry), ex);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Read Doris failed.", ex);
                }
                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    private List<PartitionDefinition> getPartitions(Object... keys) {
        readOptions.setReadFields((String.join(",", selectFields)));
        StringJoiner filter = new StringJoiner(" AND ");
        for (int i = 0; i < keys.length && i < conditionFields.length; i++) {
            filter.add(String.format("%s = '%s'", conditionFields[i], keys[i]));
        }
        readOptions.setFilterQuery(filter.toString());
        try {
            return RestService.findPartitions(options, readOptions, logger);
        } catch (DorisException ex) {
            logger.error("Failed fetch doris partitions");
            return new ArrayList<>();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @VisibleForTesting
    public Cache<RowData, List<RowData>> getCache() {
        return cache;
    }
}
