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
