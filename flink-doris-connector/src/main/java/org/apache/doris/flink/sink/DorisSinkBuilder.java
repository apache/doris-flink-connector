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
package org.apache.doris.flink.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.writer.DorisRecordSerializer;
import org.apache.flink.util.Preconditions;

public class DorisSinkBuilder<IN> {
    private DorisOptions dorisOptions;
    private DorisReadOptions dorisReadOptions;
    private DorisExecutionOptions dorisExecutionOptions;
    private DorisRecordSerializer<IN> serializer;

    public DorisSinkBuilder<IN> setDorisOptions(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
        return this;
    }

    public DorisSinkBuilder<IN> setDorisReadOptions(DorisReadOptions dorisReadOptions) {
        this.dorisReadOptions = dorisReadOptions;
        return this;
    }

    public DorisSinkBuilder<IN> setDorisExecutionOptions(DorisExecutionOptions dorisExecutionOptions) {
        this.dorisExecutionOptions = dorisExecutionOptions;
        return this;
    }

    public DorisSinkBuilder<IN> setSerializer(DorisRecordSerializer<IN> serializer) {
        this.serializer = serializer;
        return this;
    }

    public DorisSink<IN> build() {
        Preconditions.checkNotNull(dorisOptions);
        // TODO: remove read optional.
        Preconditions.checkNotNull(dorisReadOptions);
        Preconditions.checkNotNull(dorisExecutionOptions);
        Preconditions.checkNotNull(serializer);
        return new DorisSink<IN>(dorisOptions, dorisReadOptions, dorisExecutionOptions, serializer);
    }
}
