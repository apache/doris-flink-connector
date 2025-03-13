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
        // HTTP service port for web console and REST API access
        public static final int HTTP_PORT = 8030;

        // RPC port for internal communication between FEs
        public static final int RPC_PORT = 9020;

        // Query port for MySQL protocol connections
        public static final int QUERY_PORT = 9030;

        // Edit log port for Master FE log synchronization
        public static final int EDIT_LOG_PORT = 9010;

        public static final int FLIGHT_SQL_PORT = 10030;
    }

    /** BE (Backend) related ports */
    public static final class BE {

        // BE heartbeat service port for communication with FE
        public static final int HEARTBEAT_SERVICE_PORT = 9050;

        // BE Brpc port for communication between BEs
        public static final int BRPC_PORT = 8060;

        // BE Thrift service port
        public static final int THRIFT_PORT = 9060;

        // BE Webservice port
        public static final int WEBSERVICE_PORT = 8040;

        public static final int FLIGHT_SQL_PORT = 10040;
    }

    /** Broker related ports */
    public static final class Broker {
        // Broker RPC port
        public static final int RPC_PORT = 8000;
    }

    /**
     * Get list of essential Doris ports Used in E2E testing and other scenarios to check if
     * container started successfully
     *
     * @return Array of key ports
     */
    public static int[] getEssentialPorts() {
        return new int[] {
            FE.HTTP_PORT, // FE HTTP port
            FE.QUERY_PORT, // MySQL protocol port
            BE.THRIFT_PORT // BE Thrift port
        };
    }

    /**
     * Get all FE ports
     *
     * @return Array of FE ports
     */
    public static int[] getAllFEPorts() {
        return new int[] {
            FE.HTTP_PORT, FE.RPC_PORT, FE.QUERY_PORT, FE.EDIT_LOG_PORT, FE.FLIGHT_SQL_PORT
        };
    }

    /**
     * Get all BE ports
     *
     * @return Array of BE ports
     */
    public static int[] getAllBEPorts() {
        return new int[] {
            BE.HEARTBEAT_SERVICE_PORT, BE.BRPC_PORT, BE.THRIFT_PORT, BE.WEBSERVICE_PORT
        };
    }

    /**
     * Check if the specified port is a standard Doris port
     *
     * @param port Port to check
     * @return true if it's a standard Doris port
     */
    public static boolean isDorisPort(int port) {
        for (int fePort : getAllFEPorts()) {
            if (port == fePort) return true;
        }

        for (int bePort : getAllBEPorts()) {
            if (port == bePort) return true;
        }

        return port == Broker.RPC_PORT;
    }

    /**
     * Get port description
     *
     * @param port Port number
     * @return Port description
     */
    public static String getPortDescription(int port) {
        switch (port) {
            case FE.HTTP_PORT:
                return "FE HTTP Port";
            case FE.RPC_PORT:
                return "FE RPC Port";
            case FE.QUERY_PORT:
                return "FE Query Port (MySQL)";
            case FE.EDIT_LOG_PORT:
                return "FE Edit Log Port";
            case FE.FLIGHT_SQL_PORT:
                return "FE Flight SQL Port";
            case BE.HEARTBEAT_SERVICE_PORT:
                return "BE Heartbeat Service Port";
            case BE.BRPC_PORT:
                return "BE BRPC Port";
            case BE.THRIFT_PORT:
                return "BE Thrift Port";
            case BE.WEBSERVICE_PORT:
                return "BE Webservice Port";
            case Broker.RPC_PORT:
                return "Broker RPC Port";
            default:
                return "Unknown Doris Port";
        }
    }

    /**
     * Build a list of listening ports to check if container started successfully
     *
     * @return Port listening description string for E2E testing
     */
    public static String getPortsForContainerCheck() {
        int[] ports = getEssentialPorts();
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < ports.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(ports[i]);
        }
        sb.append("]");
        return sb.toString();
    }
}
