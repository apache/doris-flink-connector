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

package org.apache.doris.flink.rest;

import org.apache.doris.flink.exception.DorisException;

/** Stream load transaction state returned by Doris get_load_state API. */
public enum LoadState {
    UNKNOWN,
    PREPARE,
    PRECOMMITTED,
    COMMITTED,
    VISIBLE,
    ABORTED;

    public static LoadState fromString(String value) throws DorisException {
        try {
            return LoadState.valueOf(value.trim().toUpperCase());
        } catch (Exception e) {
            throw new DorisException("Unknown stream load transaction state: " + value, e);
        }
    }

    public boolean isPending() {
        return this == PREPARE || this == PRECOMMITTED;
    }

    public boolean isCommitted() {
        return this == COMMITTED || this == VISIBLE;
    }
}
