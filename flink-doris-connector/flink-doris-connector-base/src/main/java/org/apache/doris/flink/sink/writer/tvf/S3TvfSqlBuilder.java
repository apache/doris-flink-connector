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

package org.apache.doris.flink.sink.writer.tvf;

import org.apache.flink.util.Preconditions;

import org.apache.doris.flink.cfg.S3TvfOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import static org.apache.doris.flink.sink.writer.LoadConstants.DORIS_DELETE_SIGN;
import static org.apache.doris.flink.sink.writer.tvf.TvfSqlUtils.quoteIdentifier;
import static org.apache.doris.flink.sink.writer.tvf.TvfSqlUtils.quoteLiteral;

/** Builds one INSERT INTO SELECT FROM S3 TVF statement for a committable. */
class S3TvfSqlBuilder {

    private final S3TvfOptions options;

    public S3TvfSqlBuilder(S3TvfOptions options) {
        this.options = options;
    }

    public String buildInsertSql(S3TvfCommittable committable) {
        Preconditions.checkArgument(!committable.getObjectKeys().isEmpty());
        List<String> loadColumns = new ArrayList<>(committable.getColumns());
        if (committable.isDeleteSignEnabled()) {
            loadColumns.add(DORIS_DELETE_SIGN);
        }
        String columnSql = joinIdentifiers(loadColumns);
        String uri = buildUri(committable.getObjectKeys());

        return "INSERT INTO "
                + quoteIdentifier(committable.getDatabase())
                + "."
                + quoteIdentifier(committable.getTable())
                + " WITH LABEL "
                + quoteIdentifier(committable.getLabel())
                + " ("
                + columnSql
                + ") SELECT "
                + columnSql
                + " FROM S3("
                + property("uri", uri)
                + ","
                + property("s3.access_key", options.getAccessKey())
                + ","
                + property("s3.secret_key", options.getSecretKey())
                + ","
                + property("s3.region", options.getRegion())
                + ","
                + property("s3.endpoint", options.getEndpoint())
                + ","
                + property("format", "json")
                + ","
                + property("read_json_by_line", "true")
                + ","
                + property("use_path_style", Boolean.toString(options.isPathStyleAccess()))
                + ")";
    }

    private String buildUri(List<String> objectKeys) {
        if (objectKeys.size() == 1) {
            return "s3://" + options.getBucket() + "/" + objectKeys.get(0);
        }
        return "s3://" + options.getBucket() + "/{" + String.join(",", objectKeys) + "}";
    }

    private static String joinIdentifiers(List<String> identifiers) {
        StringJoiner joiner = new StringJoiner(",");
        for (String identifier : identifiers) {
            joiner.add(quoteIdentifier(identifier));
        }
        return joiner.toString();
    }

    private static String property(String key, String value) {
        return quoteLiteral(key) + " = " + quoteLiteral(value);
    }
}
