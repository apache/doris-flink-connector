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

public interface ConfigurationOptions {
    // doris fe node address
    String DORIS_FENODES = "fenodes";

    String DORIS_DEFAULT_CLUSTER = "default_cluster";

    String TABLE_IDENTIFIER = "table.identifier";
    String DORIS_READ_FIELD = "doris.read.field";
    String DORIS_FILTER_QUERY = "doris.filter.query";
    String DORIS_USER = "username";
    String DORIS_PASSWORD = "password";
    String DORIS_REQUEST_RETRIES = "doris.request.retries";
    String DORIS_REQUEST_CONNECT_TIMEOUT_MS = "doris.request.connect.timeout";
    String DORIS_REQUEST_READ_TIMEOUT_MS = "doris.request.read.timeout";
    String DORIS_REQUEST_QUERY_TIMEOUT_S = "doris.request.query.timeout";
    Integer DORIS_REQUEST_RETRIES_DEFAULT = 3;
    Integer DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    Integer DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;
    Integer DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT = 21600;

    String DORIS_TABLET_SIZE = "doris.request.tablet.size";
    Integer DORIS_TABLET_SIZE_DEFAULT = 1;
    Integer DORIS_TABLET_SIZE_MIN = 1;

    String DORIS_BATCH_SIZE = "doris.batch.size";
    Integer DORIS_BATCH_SIZE_DEFAULT = 4064;
    Integer DORIS_BATCH_SIZE_MAX = 65535;

    String DORIS_EXEC_MEM_LIMIT = "doris.exec.mem.limit";
    Long DORIS_EXEC_MEM_LIMIT_DEFAULT = 8589934592L;
    String DORIS_EXEC_MEM_LIMIT_DEFAULT_STR = "8192mb";
    String DORIS_DESERIALIZE_ARROW_ASYNC = "doris.deserialize.arrow.async";
    Boolean DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT = false;
    String DORIS_DESERIALIZE_QUEUE_SIZE = "doris.deserialize.queue.size";
    Integer DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT = 64;

    String USE_FLIGHT_SQL = "source.use-flight-sql";
    Boolean USE_FLIGHT_SQL_DEFAULT = false;

    String FLIGHT_SQL_PORT = "source.flight-sql-port";
    Integer FLIGHT_SQL_PORT_DEFAULT = 9040;
}
