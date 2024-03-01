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

package org.apache.doris.flink.sink.committer;

import java.io.Serializable;

/**
 * The strategy for Commit transaction failure, the default is NEVER. NEVER: Do not ignore Commit
 * transaction failure; ONCE: When resuming the task for the first time, ignore the Txn that failed
 * the first Commit, For example, the Txn has expired; ALWAYS: Ignore Commit transaction failure
 */
public enum CommitTolerance implements Serializable {
    NEVER,
    ONCE,
    ALWAYS;

    public static CommitTolerance of(String name) {
        try {
            return CommitTolerance.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported commit tolerance: " + name);
        }
    }
}
