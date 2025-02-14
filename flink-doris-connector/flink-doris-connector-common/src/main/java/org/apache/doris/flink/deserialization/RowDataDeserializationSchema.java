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

package org.apache.doris.flink.deserialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.apache.doris.flink.deserialization.converter.DorisRowConverter;

import java.util.List;

/**
 * A simple implementation of {@link DorisDeserializationSchema} which converts the received list
 * record into {@link GenericRowData}.
 */
@PublicEvolving
public class RowDataDeserializationSchema implements DorisDeserializationSchema<RowData> {

    private final DorisRowConverter rowConverter;

    public RowDataDeserializationSchema(RowType rowType) {
        this.rowConverter = new DorisRowConverter(rowType);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }

    @Override
    public void deserialize(List<?> record, Collector<RowData> out) throws Exception {
        RowData row = rowConverter.convertInternal(record);
        out.collect(row);
    }
}
