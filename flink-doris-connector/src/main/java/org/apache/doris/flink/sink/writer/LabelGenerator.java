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
package org.apache.doris.flink.sink.writer;

import org.apache.flink.util.Preconditions;

import java.util.UUID;

/**
 * Generator label for stream load.
 */
public class LabelGenerator {

    private final static int MAX_LOAD_LABEL_LENGTH = 128;
    private String labelPrefix;
    private boolean enable2PC;
    private String tableIdentifier;
    private int subtaskId;

    public LabelGenerator(String labelPrefix, boolean enable2PC) {
        this.labelPrefix = labelPrefix;
        this.enable2PC = enable2PC;
    }

    public LabelGenerator(String labelPrefix, boolean enable2PC, String tableIdentifier, int subtaskId) {
        this(labelPrefix, enable2PC);
        // The label of stream load can not contain `.`
        this.tableIdentifier = tableIdentifier.replace(".", "_");
        this.subtaskId = subtaskId;
    }

    public String generateLabel(long chkId) {
        String label = String.format("%s_%s_%s", labelPrefix, subtaskId, chkId);
        String result = enable2PC ? label : label + "_" + UUID.randomUUID();
        return generateLabelUtil(result);
    }

    public String generateTableLabel(long chkId) {
        Preconditions.checkState(tableIdentifier != null);
        String label = String.format("%s_%s_%s_%s", labelPrefix, tableIdentifier, subtaskId, chkId);
        String result = enable2PC ? label : label + "_" + UUID.randomUUID();
        return generateLabelUtil(result);
    }

    public String generateBatchLabel() {
        return generateLabelUtil(labelPrefix + "_" + UUID.randomUUID());
    }

    public String generateBatchLabel(String table) {
        return generateLabelUtil(String.format("%s_%s_%s", labelPrefix, table, UUID.randomUUID()));
    }

    public String generateLabelUtil(String label) {
        int len = label.length();
        if (len <= MAX_LOAD_LABEL_LENGTH) {
            return label;
        } else {
            return label.substring(len - MAX_LOAD_LABEL_LENGTH);
        }
    }
}
