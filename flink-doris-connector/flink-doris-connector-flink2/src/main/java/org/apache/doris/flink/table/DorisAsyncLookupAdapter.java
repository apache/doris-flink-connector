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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;

import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.deserialization.converter.DorisRowConverter;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Adapter that bridges Flink 2.2 {@link AsyncLookupFunction} to the existing {@link
 * DorisRowDataAsyncLookupFunction} which is implemented against the older {@link
 * org.apache.flink.table.functions.AsyncTableFunction} API and shared with Flink 1.15–1.20.
 */
public class DorisAsyncLookupAdapter extends AsyncLookupFunction {

    private final DorisRowDataAsyncLookupFunction delegate;
    private final DorisRowConverter keyConverter;
    private final int[] keyIndex;

    public DorisAsyncLookupAdapter(
            DorisOptions options,
            DorisLookupOptions lookupOptions,
            String[] selectFields,
            DataType[] fieldTypes,
            String[] conditionFields,
            int[] keyIndex) {
        this.delegate =
                new DorisRowDataAsyncLookupFunction(
                        options,
                        lookupOptions,
                        selectFields,
                        fieldTypes,
                        conditionFields,
                        keyIndex);
        this.keyIndex = keyIndex;
        DataType[] keyTypes = new DataType[keyIndex.length];
        for (int i = 0; i < keyIndex.length; i++) {
            keyTypes[i] = fieldTypes[keyIndex[i]];
        }
        this.keyConverter = new DorisRowConverter(keyTypes);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        delegate.open(context);
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        Object[] keys = new Object[keyIndex.length];
        for (int i = 0; i < keyIndex.length; i++) {
            keys[i] = keyConverter.convertExternal(keyRow, i);
        }

        CompletableFuture<Collection<RowData>> future = new CompletableFuture<>();
        try {
            delegate.eval(future, keys);
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public void close() throws Exception {
        try {
            delegate.close();
        } finally {
            super.close();
        }
    }
}
