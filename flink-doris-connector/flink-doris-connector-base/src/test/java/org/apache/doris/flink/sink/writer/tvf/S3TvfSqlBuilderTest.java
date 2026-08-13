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

import org.apache.doris.flink.cfg.S3TvfOptions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class S3TvfSqlBuilderTest {

    @Test
    public void testBuildInsertSqlWithExactObjectListAndDeleteSign() {
        S3TvfOptions options =
                S3TvfOptions.builder()
                        .setEndpoint("https://s3.example.com")
                        .setRegion("us-east-1")
                        .setBucket("bucket")
                        .setPrefix("prefix")
                        .setAccessKey("ak")
                        .setSecretKey("sk")
                        .setPathStyleAccess(true)
                        .build();
        S3TvfCommittable committable =
                new S3TvfCommittable(
                        7L,
                        "db",
                        "tbl",
                        "label_tbl_7",
                        Arrays.asList("prefix_tbl_0_7_0.json", "prefix_tbl_1_7_0.json"),
                        Arrays.asList("id", "name"),
                        true);

        String sql = new S3TvfSqlBuilder(options).buildInsertSql(committable);

        Assert.assertEquals(
                "INSERT INTO `db`.`tbl` WITH LABEL `label_tbl_7` "
                        + "(`id`,`name`,`__DORIS_DELETE_SIGN__`) "
                        + "SELECT `id`,`name`,`__DORIS_DELETE_SIGN__` FROM S3("
                        + "'uri' = 's3://bucket/{prefix_tbl_0_7_0.json,prefix_tbl_1_7_0.json}',"
                        + "'s3.access_key' = 'ak','s3.secret_key' = 'sk',"
                        + "'s3.region' = 'us-east-1','s3.endpoint' = 'https://s3.example.com',"
                        + "'format' = 'json','read_json_by_line' = 'true',"
                        + "'use_path_style' = 'true')",
                sql);
    }
}
