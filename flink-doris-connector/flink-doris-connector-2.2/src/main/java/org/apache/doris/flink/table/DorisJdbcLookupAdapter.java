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
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;

import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.deserialization.converter.DorisRowConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Adapter that bridges Flink 2.2 {@link LookupFunction} to the existing {@link
 * DorisRowDataJdbcLookupFunction} which is implemented against the older {@link
 * org.apache.flink.table.functions.TableFunction} API and shared with Flink 1.15–1.20.
 */
public class DorisJdbcLookupAdapter extends LookupFunction {

    private final DorisRowDataJdbcLookupFunction delegate;
    private final DorisRowConverter keyConverter;
    private final int[] keyIndex;

    public DorisJdbcLookupAdapter(
            DorisOptions options,
            DorisLookupOptions lookupOptions,
            String[] selectFields,
            DataType[] fieldTypes,
            String[] conditionFields,
            int[] keyIndex) {
        this.delegate =
                new DorisRowDataJdbcLookupFunction(
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
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        final List<RowData> results = new ArrayList<>();
        delegate.setCollector(
                new Collector<RowData>() {
                    @Override
                    public void collect(RowData record) {
                        results.add(record);
                    }

                    @Override
                    public void close() {}
                });

        delegate.eval(keyRow);
        return results;
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
