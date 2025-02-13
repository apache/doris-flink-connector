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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.apache.doris.flink.cfg.DorisLookupOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.deserialization.converter.DorisRowConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class DorisJdbcLookupReader extends DorisLookupReader {

    private static final Logger LOG = LoggerFactory.getLogger(DorisJdbcLookupReader.class);

    private ExecutionPool pool;

    private DorisRowConverter converter;

    private DorisRowConverter keyConverter;

    private LookupSchema schema;

    public DorisJdbcLookupReader(
            DorisOptions options, DorisLookupOptions lookupOptions, LookupSchema lookupSchema) {
        this.converter = new DorisRowConverter(lookupSchema.getFieldTypes());
        this.pool = new ExecutionPool(options, lookupOptions);
        this.schema = lookupSchema;
        this.keyConverter = buildKeyConvert();
    }

    private DorisRowConverter buildKeyConvert() {
        int[] keyIndex = schema.getKeyIndex();
        DataType[] keyTypes = new DataType[keyIndex.length];
        DataType[] fieldTypes = schema.getFieldTypes();
        for (int i = 0; i < keyIndex.length; i++) {
            keyTypes[i] = fieldTypes[keyIndex[i]];
        }
        return new DorisRowConverter(keyTypes);
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncGet(RowData recordIn) throws IOException {
        CompletableFuture<Collection<RowData>> result = new CompletableFuture<>();
        Record record = convertRecord(recordIn);
        try {
            pool.get(new Get(record))
                    .handleAsync(
                            (resultRow, throwable) -> {
                                try {
                                    if (throwable != null) {
                                        result.completeExceptionally(throwable);
                                    } else {
                                        if (resultRow == null) {
                                            result.complete(new ArrayList<>());
                                        } else {
                                            // convert Record to RowData
                                            List<RowData> rowDatas = convertRowDataList(resultRow);
                                            result.complete(rowDatas);
                                        }
                                    }
                                } catch (Throwable e) {
                                    result.completeExceptionally(e);
                                }
                                return null;
                            });
        } catch (Exception e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    private List<RowData> convertRowDataList(List<Record> records) {
        List<RowData> results = new ArrayList<>();
        for (Record record : records) {
            RowData rowData = convertRowData(record);
            results.add(rowData);
        }
        return results;
    }

    private RowData convertRowData(Record record) {
        if (record == null) {
            return null;
        }
        Object[] values = record.getValues();
        GenericRowData rowData = converter.convertInternal(Arrays.asList(values));
        return rowData;
    }

    private Record convertRecord(RowData recordIn) {
        Record record = new Record(schema);
        int[] keyIndex = schema.getKeyIndex();

        for (int index = 0; index < keyIndex.length; index++) {
            Object value = keyConverter.convertExternal(recordIn, index);
            record.setObject(keyIndex[index], value);
        }
        return record;
    }

    @Override
    public Collection<RowData> get(RowData record) throws IOException {
        try {
            return this.asyncGet(record).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (this.pool != null) {
            this.pool.close();
        }
    }
}
