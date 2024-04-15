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

import java.util.regex.Pattern;

/** util for handle response. */
public class ResponseUtil {
    public static final Pattern LABEL_EXIST_PATTERN =
            Pattern.compile("Label \\[(.*)\\] has already been used, relate to txn \\[(\\d+)\\]");
    public static final Pattern COMMITTED_PATTERN =
            Pattern.compile(
                    "transaction \\[(\\d+)\\] is already \\b(COMMITTED|committed|VISIBLE|visible)\\b, not pre-committed.");

    public static final Pattern ABORTTED_PATTERN =
            Pattern.compile(
                    "transaction \\[(\\d+)\\] is already|transaction \\[(\\d+)\\] not found");

    public static boolean isCommitted(String msg) {
        return COMMITTED_PATTERN.matcher(msg).find();
    }

    public static boolean isAborted(String msg) {
        return ABORTTED_PATTERN.matcher(msg).find();
    }

    static final Pattern COPY_COMMITTED_PATTERN =
            Pattern.compile("errCode = 2, detailMessage = No files can be copied.*");

    public static boolean isCopyCommitted(String msg) {
        return COPY_COMMITTED_PATTERN.matcher(msg).find();
    }
}
