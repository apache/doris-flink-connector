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

package org.apache.doris.flink.container.config;

/**
 * DorisPorts class contains default port configurations for Apache Doris Can be used in E2E testing
 * and other scenarios requiring access to Doris services
 */
public class DorisPorts {
    /** FE (Frontend) related ports */
    public static final class FE {

        public static final int HTTP_PORT = 8030;
        public static final int QUERY_PORT = 9030;
        public static final int FLIGHT_SQL_PORT = 10030;
    }

    /** BE (Backend) related ports */
    public static final class BE {

        public static final int THRIFT_PORT = 9060;
        public static final int WEBSERVICE_PORT = 8040;
        public static final int FLIGHT_SQL_PORT = 10040;
    }
}
