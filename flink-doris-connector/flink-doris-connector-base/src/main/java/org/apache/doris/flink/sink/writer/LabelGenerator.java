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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.regex.Pattern;

/** Generator label for stream load. */
public class LabelGenerator {
    // doris default label regex
    private static final String LABEL_REGEX = "^[-_A-Za-z0-9:]{1,128}$";
    private static final int MAX_LABEL_LENGTH = 128;
    private static final int DIGEST_LENGTH = 32;
    private static final Pattern LABEL_PATTERN = Pattern.compile(LABEL_REGEX);
    private String labelPrefix;
    private boolean enable2PC;
    private String tableIdentifier;
    private String originalTableIdentifier;
    private int subtaskId;

    public LabelGenerator(String labelPrefix, boolean enable2PC) {
        this.labelPrefix = labelPrefix;
        this.enable2PC = enable2PC;
    }

    public LabelGenerator(
            String labelPrefix, boolean enable2PC, String tableIdentifier, int subtaskId) {
        this(labelPrefix, enable2PC);
        // The label of stream load can not contain `.`
        this.originalTableIdentifier = tableIdentifier;
        this.tableIdentifier = tableIdentifier.replace(".", "_");
        this.subtaskId = subtaskId;
    }

    public LabelGenerator(String labelPrefix, boolean enable2PC, int subtaskId) {
        this.labelPrefix = labelPrefix;
        this.enable2PC = enable2PC;
        this.subtaskId = subtaskId;
    }

    public String generateTableLabel(long chkId) {
        Preconditions.checkState(tableIdentifier != null);
        String label = String.format("%s_%s_%s_%s", labelPrefix, tableIdentifier, subtaskId, chkId);

        if (!enable2PC) {
            label = label + "_" + UUID.randomUUID();
        }

        if (LABEL_PATTERN.matcher(label).matches()) {
            // The unicode table name or length exceeds the limit
            return label;
        }

        if (enable2PC) {
            return generateStableFallbackLabel(chkId);
        } else {
            return String.format("%s_%s_%s_%s", labelPrefix, subtaskId, chkId, UUID.randomUUID());
        }
    }

    public String generateBatchLabel(String table) {
        String uuid = UUID.randomUUID().toString();
        String label = String.format("%s_%s_%s", labelPrefix, table, uuid);
        if (!LABEL_PATTERN.matcher(label).matches()) {
            return labelPrefix + "_" + uuid;
        }
        return label;
    }

    public String generateCopyBatchLabel(String table, long chkId, int fileNum) {
        return String.format("%s_%s_%s_%s_%s", labelPrefix, table, subtaskId, chkId, fileNum);
    }

    public String getConcatLabelPrefix() {
        String concatPrefix = String.format("%s_%s_%s", labelPrefix, tableIdentifier, subtaskId);
        return concatPrefix;
    }

    private String generateStableFallbackLabel(long chkId) {
        String digest = stableDigest(chkId);
        String suffix = String.format("_%s_%s_%s", digest, subtaskId, chkId);
        String safePrefix = sanitizeLabelPart(labelPrefix);
        int maxPrefixLength = MAX_LABEL_LENGTH - suffix.length();
        Preconditions.checkState(maxPrefixLength > 0);
        if (safePrefix.length() > maxPrefixLength) {
            safePrefix = safePrefix.substring(0, maxPrefixLength);
        }
        String label = safePrefix + suffix;
        Preconditions.checkState(LABEL_PATTERN.matcher(label).matches());
        return label;
    }

    private String stableDigest(long chkId) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            updateDigest(digest, "flink-doris-stream-load-2pc");
            updateDigest(digest, labelPrefix);
            updateDigest(
                    digest,
                    originalTableIdentifier == null ? tableIdentifier : originalTableIdentifier);
            updateDigest(digest, String.valueOf(subtaskId));
            updateDigest(digest, String.valueOf(chkId));
            return toHex(digest.digest()).substring(0, DIGEST_LENGTH);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private void updateDigest(MessageDigest digest, String value) {
        if (value != null) {
            digest.update(value.getBytes(StandardCharsets.UTF_8));
        }
        digest.update((byte) 0);
    }

    private String toHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        char[] hexArray = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            hexChars[i * 2] = hexArray[v >>> 4];
            hexChars[i * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    private String sanitizeLabelPart(String value) {
        if (value == null || value.isEmpty()) {
            return "label";
        }
        StringBuilder builder = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '-' || ch == '_' || ch == ':' || Character.isLetterOrDigit(ch)) {
                if (ch < 128) {
                    builder.append(ch);
                } else {
                    builder.append('_');
                }
            } else {
                builder.append('_');
            }
        }
        return builder.length() == 0 ? "label" : builder.toString();
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }
}
