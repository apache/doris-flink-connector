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

package org.apache.doris.flink.source.split;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import org.apache.doris.flink.source.reader.DorisValueReader;
import org.apache.doris.flink.source.reader.ValueReader;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * An implementation of {@link RecordsWithSplitIds}. This is essentially a slim wrapper around the
 * {@link DorisValueReader} that only adds information about the current split, or finished splits
 */
public class DorisSplitRecords implements RecordsWithSplitIds<List> {

    private final Set<String> finishedSplits;
    private final ValueReader valueReader;
    private String splitId;

    public DorisSplitRecords(String splitId, ValueReader valueReader, Set<String> finishedSplits) {
        this.splitId = splitId;
        this.valueReader = valueReader;
        this.finishedSplits = finishedSplits;
    }

    public static DorisSplitRecords forRecords(
            final String splitId, final ValueReader valueReader) {
        return new DorisSplitRecords(splitId, valueReader, Collections.emptySet());
    }

    public static DorisSplitRecords finishedSplit(final String splitId) {
        return new DorisSplitRecords(null, null, Collections.singleton(splitId));
    }

    @Nullable
    @Override
    public String nextSplit() {
        // move the split one (from current value to null)
        final String nextSplit = this.splitId;
        this.splitId = null;
        if (valueReader == null || !valueReader.hasNext()) {
            return null;
        }
        return nextSplit;
    }

    @Nullable
    @Override
    public List nextRecordFromSplit() {
        if (valueReader != null && valueReader.hasNext()) {
            List next = valueReader.next();
            return next;
        }
        return null;
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSplits;
    }
}
