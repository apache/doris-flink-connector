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

package org.apache.doris.flink.sink.overwrite;

import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.doris.flink.sink.DorisAbstractCommittable;

/** Finalizes bounded INSERT OVERWRITE after all post-commit messages are consumed. */
public class DorisOverwriteFinalizerOperator extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<CommittableMessage<DorisAbstractCommittable>, Void>,
                BoundedOneInput {
    private static final long serialVersionUID = 1L;

    private final DorisOverwriteOptions overwriteOptions;

    public DorisOverwriteFinalizerOperator(DorisOverwriteOptions overwriteOptions) {
        this.overwriteOptions = overwriteOptions;
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<DorisAbstractCommittable>> element) {
        // The post-commit topology only needs the bounded end-of-input signal.
    }

    @Override
    public void endInput() {
        DorisOverwriteManager.finalizeOverwrite(overwriteOptions);
    }
}
