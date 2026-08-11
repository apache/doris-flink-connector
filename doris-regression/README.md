<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Doris Regression Cases

This standalone Maven project packages the Flink connector programs used by the Apache Doris
regression suite. It is intentionally kept outside the connector Maven reactor and pins the
connector version in `pom.xml`.

## Build

```shell
mvn clean package
```

The shaded artifact is generated at `target/flink-doris-case.jar`.

## Entry Points

| Main class | Apache Doris regression case | Case-specific argument |
| --- | --- | --- |
| `org.apache.doris.DorisFlinkDfSinkDemo` | `regression-test/suites/flink_connector_p0/flink_connector.groovy` | `--doris-table-identifier <database.table>` |
| `org.apache.doris.FlinkConnectorTypeCase` | `regression-test/suites/flink_connector_p0/flink_connector_type.groovy` | `--doris-database <database>` |
| `org.apache.doris.DatabaseFullSync` | `regression-test/suites/flink_connector_p0/flink_connector_syncdb.groovy` | `--doris-database <database>` |

All arguments are passed as name-value pairs. The common required arguments are:

```text
--doris-fe-address <host:port>
--doris-user <user>
--doris-password <password>
```

The optional TLS arguments are shared by all entry points:

```text
--doris-enable-tls <true|false>
--doris-tls-ca-certificate-path <path>
--doris-tls-skip-hostname-verification <true|false>
--doris-tls-excluded-protocols <http,mysql,thrift,arrowflight>
```

An empty value for `--doris-tls-excluded-protocols` enables TLS for every supported protocol.
