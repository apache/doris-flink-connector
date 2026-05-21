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

/** Builds Doris HTTP REST URLs with a shared protocol switch. */
public class DorisUrlBuilder {
    private final boolean enableHttps;

    public DorisUrlBuilder(boolean enableHttps) {
        this.enableHttps = enableHttps;
    }

    public String baseUrl(String hostPort) {
        return scheme() + "://" + hostPort;
    }

    public String streamLoad(String hostPort, String db, String table) {
        return String.format("%s/api/%s/%s/_stream_load", baseUrl(hostPort), db, table);
    }

    public String streamLoad2pc(String hostPort, String db) {
        return String.format("%s/api/%s/_stream_load_2pc", baseUrl(hostPort), db);
    }

    public String tableSchema(String hostPort, String db, String table) {
        return String.format("%s/api/%s/%s/_schema", baseUrl(hostPort), db, table);
    }

    public String catalogTableSchema(String hostPort, String catalog, String db, String table) {
        return String.format("%s/api/%s/%s/%s/_schema", baseUrl(hostPort), catalog, db, table);
    }

    public String queryPlan(String hostPort, String db, String table) {
        return String.format("%s/api/%s/%s/_query_plan", baseUrl(hostPort), db, table);
    }

    public String informationSchemaQuery(String hostPort) {
        return baseUrl(hostPort) + "/api/query/default_cluster/information_schema";
    }

    public String schemaChange(String hostPort, String database) {
        return String.format("%s/api/query/default_cluster/%s", baseUrl(hostPort), database);
    }

    public String lightSchemaChange(String hostPort, String database, String table) {
        return String.format(
                "%s/api/enable_light_schema_change/%s/%s", baseUrl(hostPort), database, table);
    }

    public String backends(String hostPort) {
        return baseUrl(hostPort) + "/api/backends?is_alive=true";
    }

    public String copyUpload(String hostPort) {
        return baseUrl(hostPort) + "/copy/upload";
    }

    public String copyQuery(String hostPort) {
        return baseUrl(hostPort) + "/copy/query";
    }

    private String scheme() {
        return enableHttps ? "https" : "http";
    }
}
