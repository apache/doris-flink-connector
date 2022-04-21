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

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;

/**
 * The builder class for {@link DorisSource} to make it easier for the users to construct a {@link
 * DorisSource}.
 **/
public class DorisSourceBuilder<OUT> {

    private DorisOptions options;
    private DorisReadOptions readOptions;

    // Boundedness
    private Boundedness boundedness;
    private DorisDeserializationSchema<OUT> deserializer;

    DorisSourceBuilder() {
        boundedness = Boundedness.BOUNDED;
    }

    public static <OUT> DorisSourceBuilder<OUT> builder() {
        return new DorisSourceBuilder();
    }

    public DorisSourceBuilder<OUT> setDorisOptions(DorisOptions options) {
        this.options = options;
        return this;
    }

    public DorisSourceBuilder<OUT> setDorisReadOptions(DorisReadOptions readOptions) {
        this.readOptions = readOptions;
        return this;
    }

    public DorisSourceBuilder<OUT> setBoundedness(Boundedness boundedness) {
        this.boundedness = boundedness;
        return this;
    }

    public DorisSourceBuilder<OUT> setDeserializer(DorisDeserializationSchema<OUT> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    public DorisSource<OUT> build() {
        return new DorisSource<>(options, readOptions, boundedness, deserializer);
    }
}
