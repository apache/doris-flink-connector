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

package org.apache.doris.flink.cfg;

import java.io.Serializable;
import java.util.Objects;

/** Options for writing staged files and loading them through the S3 TVF. */
public class S3TvfOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String endpoint;
    private final String region;
    private final String bucket;
    private final String prefix;
    private final String accessKey;
    private final String secretKey;
    private final boolean pathStyleAccess;

    private S3TvfOptions(Builder builder) {
        this.endpoint = builder.endpoint;
        this.region = builder.region;
        this.bucket = builder.bucket;
        this.prefix = builder.prefix;
        this.accessKey = builder.accessKey;
        this.secretKey = builder.secretKey;
        this.pathStyleAccess = builder.pathStyleAccess;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public boolean isPathStyleAccess() {
        return pathStyleAccess;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        S3TvfOptions that = (S3TvfOptions) o;
        return pathStyleAccess == that.pathStyleAccess
                && Objects.equals(endpoint, that.endpoint)
                && Objects.equals(region, that.region)
                && Objects.equals(bucket, that.bucket)
                && Objects.equals(prefix, that.prefix)
                && Objects.equals(accessKey, that.accessKey)
                && Objects.equals(secretKey, that.secretKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                endpoint, region, bucket, prefix, accessKey, secretKey, pathStyleAccess);
    }

    @Override
    public String toString() {
        return "S3TvfOptions{"
                + "endpoint='"
                + endpoint
                + '\''
                + ", region='"
                + region
                + '\''
                + ", bucket='"
                + bucket
                + '\''
                + ", prefix='"
                + prefix
                + '\''
                + ", pathStyleAccess="
                + pathStyleAccess
                + '}';
    }

    /** Builder for {@link S3TvfOptions}. */
    public static class Builder {
        private String endpoint;
        private String region;
        private String bucket;
        private String prefix;
        private String accessKey;
        private String secretKey;
        private boolean pathStyleAccess;

        public Builder setEndpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder setRegion(String region) {
            this.region = region;
            return this;
        }

        public Builder setBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder setPrefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder setAccessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder setSecretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder setPathStyleAccess(boolean pathStyleAccess) {
            this.pathStyleAccess = pathStyleAccess;
            return this;
        }

        public S3TvfOptions build() {
            if (prefix != null) {
                for (char character : "*?[]{},\\".toCharArray()) {
                    if (prefix.indexOf(character) >= 0) {
                        throw new IllegalArgumentException(
                                "sink.s3.prefix must not contain glob characters: * ? [ ] { } , \\");
                    }
                }
            }
            return new S3TvfOptions(this);
        }
    }
}
