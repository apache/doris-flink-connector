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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.lookup.DorisJdbcLookupReader;
import org.apache.doris.flink.lookup.DorisLookupFunction;
import org.apache.doris.flink.lookup.DorisLookupReader;
import org.apache.doris.flink.lookup.LookupMetrics;
import org.apache.doris.flink.lookup.LookupSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/** lookup {@link LookupFunction} for after 1.16 */
public class DorisRowDataJdbcLookupFunction extends LookupFunction implements DorisLookupFunction {
    private static final Logger LOG = LoggerFactory.getLogger(DorisRowDataJdbcLookupFunction.class);
    private final DorisOptions options;
    private final DorisLookupOptions lookupOptions;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private transient Cache<RowData, Collection<RowData>> cache;
    private DorisLookupReader lookupReader;
    private LookupSchema lookupSchema;
    private LookupMetrics lookupMetrics;

    public DorisRowDataJdbcLookupFunction(
            DorisOptions options,
            DorisLookupOptions lookupOptions,
            String[] selectFields,
            DataType[] fieldTypes,
            String[] conditionFields,
            int[] keyIndex) {
        Preconditions.checkNotNull(
                options.getJdbcUrl(), "jdbc-url is required in jdbc mode lookup");
        this.options = options;
        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.lookupOptions = lookupOptions;
        this.lookupSchema =
                new LookupSchema(
                        options.getTableIdentifier(),
                        selectFields,
                        fieldTypes,
                        conditionFields,
                        keyIndex);
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
        this.lookupReader = new DorisJdbcLookupReader(options, lookupOptions, lookupSchema);
        this.lookupMetrics = new LookupMetrics(context.getMetricGroup());
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        if (cache != null) {
            Collection<RowData> cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {
                lookupMetrics.incHitCount();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("lookup cache hit for key: {}", keyRow);
                }
                return cachedRows;
            } else {
                lookupMetrics.incMissCount();
            }
        }
        return queryRecord(keyRow);
    }

    private Collection<RowData> queryRecord(RowData keyRow) throws IOException {
        Collection<RowData> rowData = lookupReader.get(keyRow);
        if (rowData == null) {
            rowData = Collections.emptyList();
        }
        if (cache != null) {
            cache.put(keyRow, rowData);
            lookupMetrics.incLoadCount();
        }
        return rowData;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (lookupReader != null) {
            lookupReader.close();
        }
    }

    @VisibleForTesting
    public Cache<RowData, Collection<RowData>> getCache() {
        return cache;
    }
}
