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
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

import org.apache.doris.flink.source.reader.DorisSourceRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The deserialization schema describes how to turn a Doris source record into data types
 * (Java/Scala objects) that are processed by Flink.
 */
@PublicEvolving
public interface DorisDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

    void deserialize(List<?> record, Collector<T> out) throws Exception;

    /**
     * Deserializes a Doris source record including its changelog metadata.
     *
     * <p>The default implementation preserves compatibility with existing deserialization schemas
     * by passing only a mutable list of field values to {@link #deserialize(List, Collector)}.
     */
    default void deserialize(DorisSourceRecord record, Collector<T> out) throws Exception {
        deserialize(new ArrayList<>(record.getFieldValues()), out);
    }
}
