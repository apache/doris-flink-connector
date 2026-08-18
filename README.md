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

# Flink Connector for Apache Doris 

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Join the Doris Community at Slack](https://img.shields.io/badge/chat-slack-brightgreen)](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-11jb8gesh-7IukzSrdea6mqoG0HB4gZg)

## Flink Doris Connector

Flink Doris Connector supports the following Flink versions:

| Flink Version | Runtime JDK                    |
|:--------------|:-------------------------------|
| 1.15.x        | JDK 8                          |
| 1.16.x        | JDK 8                          |
| 1.17.x        | JDK 8                          |
| 1.18.x        | JDK 8                          |
| 1.19.x        | JDK 8                          |
| 1.20.x        | JDK 8                          |
| 2.0.x         | JDK 17                         |
| 2.1.x         | JDK 17; JDK 21 (experimental) |
| 2.2.x         | JDK 17; JDK 21 (experimental) |

Flink 2.x artifacts are compiled with Java 17 (`--release 17`, classfile major version 61) and can be built with JDK 17 or JDK 21. JDK 21 runtime compatibility is experimental for the Flink versions listed above; JDK 17 remains the recommended runtime.

If you wish to contribute or use a connector from flink 1.13 (and earlier), please use the [branch-for-flink-before-1.13](https://github.com/apache/doris-flink-connector/tree/branch-for-flink-before-1.13)

More information about compilation and usage is available in the [Flink Doris Connector documentation](https://doris.apache.org/docs/dev/ecosystem/flink-doris-connector). The website documentation is maintained outside this repository and requires a separate change to keep its compatibility guidance in sync.

### TLS Configuration

The connector can apply one TLS policy to Doris HTTP, MySQL/JDBC, BE Thrift, and Arrow Flight SQL connections:

```properties
doris.enable.tls=true
doris.tls.ca-certificate-path=/etc/doris-tls/ca-chain.pem
doris.tls.skip-hostname-verification=false
doris.tls.excluded-protocols=
```

`doris.enable.tls` is disabled by default. When TLS is enabled and the CA path is empty, the connector uses the JVM or protocol driver's default trust store. When a CA path is configured, it must point to a PEM CA certificate chain on the local filesystem of every process that connects to Doris. Hostname verification remains enabled by default.

`doris.tls.excluded-protocols` accepts a comma-separated subset of `http`, `mysql`, `thrift`, and `arrowflight`. Use it only when the corresponding Doris server protocol is excluded from TLS. For example, add `thrift` when the BE Thrift service used by the non-Flight source remains plaintext. The connector does not probe protocols or fall back to plaintext after a TLS failure. When Connector-managed MySQL TLS is active, do not also put TLS properties such as `sslMode`, `useSSL`, or trust-store properties in `jdbc-url`. Arrow Flight TLS does not support the connector's hostname-only skip policy, so `doris.tls.skip-hostname-verification=true` fails fast unless `arrowflight` is excluded.

This release supports one-way TLS only: the connector verifies the Doris server certificate but does not provide a client certificate or private key. It therefore cannot connect to a Doris endpoint configured to require client certificates.

The CA path is a local file path, not an HDFS or HTTP URI. For standalone deployments, provision the file at the same path on all relevant JobManager, TaskManager, and SQL Gateway hosts. For YARN, localize the file with `yarn.ship-files` and configure its container-local relative path. For Kubernetes, mount a ConfigMap or Secret into the relevant pods at a consistent absolute path. A process that opens Catalog/JDBC connections must also be able to read the file.

## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

## How to Build

You need to copy customer_env.sh.tpl to customer_env.sh before build and you need to configure it before build.

> **Build JDK requirement:** Use JDK 8 for Flink 1.x. Use JDK 17 or JDK 21 for Flink 2.x; the resulting Flink 2.x artifacts still target Java 17.

```shell
git clone git@github.com:apache/doris-flink-connector.git
cd doris-flink-connector/flink-doris-connector
./build.sh
```

![how-to-build](https://user-images.githubusercontent.com/13284744/223990851-9d82b599-ef36-4fd1-82c8-17bdd2de22e6.gif)

### Code Style

#### Code Formatting

You need to install the google-java-format plugin. Spotless together with google-java-format is used to format the codes.

It is recommended to automatically format your code by applying the following settings:

1. Go to "Settings" → "Other Settings" → "google-java-format Settings".
2. Tick the checkbox to enable the plugin.
3. Change the code style to "Android Open Source Project (AOSP) style".
4. Go to "Settings" → "Tools" → "Actions on Save".
5. Under "Formatting Actions", select "Optimize imports" and "Reformat file".
6. From the "All file types list" next to "Reformat code", select "Java".

For earlier IntelliJ IDEA versions, the step 4 to 7 will be changed as follows.

- 4.Go to "Settings" → "Other Settings" → "Save Actions".
- 5.Under "General", enable your preferred settings for when to format the code, e.g. "Activate save actions on save".
- 6.Under "Formatting Actions", select "Optimize imports" and "Reformat file".
- 7.Under "File Path Inclusions", add an entry for `.*\.java` to avoid formatting other file types.
  Then the whole project could be formatted by command `mvn spotless:apply`.

#### Checkstyle

Checkstyle is used to enforce static coding guidelines.

1. Go to "Settings" → "Tools" → "Checkstyle".
2. Set "Scan Scope" to "Only Java sources (including tests)".
3. For "Checkstyle Version" select "8.14".
4. Under "Configuration File" click the "+" icon to add a new configuration.
5. Set "Description" to "doris-flink-connector".
6. Select "Use a local Checkstyle file" and link it to the file `tools/maven/checkstyle.xml` which is located within your cloned repository.
7. Select "Store relative to project location" and click "Next".
8. Configure the property `checkstyle.suppressions.file` with the value `suppressions.xml` and click "Next".
9. Click "Finish".
10. Select "doris-flink-connector" as the only active configuration file and click "Apply".

You can now import the Checkstyle configuration for the Java code formatter.

1. Go to "Settings" → "Editor" → "Code Style" → "Java".
2. Click the gear icon next to "Scheme" and select "Import Scheme" → "Checkstyle Configuration".
3. Navigate to and select `tools/maven/checkstyle.xml` located within your cloned repository.

Then you could click "View" → "Tool Windows" → "Checkstyle" and find the "Check Module" button in the opened tool window to validate checkstyle. Or you can use the command `mvn clean compile checkstyle:checkstyle` to validate.


## Report issues or submit pull request

If you find any bugs, feel free to file a [GitHub issue](https://github.com/apache/doris/issues) or fix it by submitting a [pull request](https://github.com/apache/doris/pulls).

## Contact Us

Contact us through the following mailing list.

| Name                                                                          | Scope                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Development-related discussions | [Subscribe](mailto:dev-subscribe@doris.apache.org)   | [Unsubscribe](mailto:dev-unsubscribe@doris.apache.org)   | [Archives](https://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## Links

* Doris official site - <https://doris.apache.org>
* Developer Mailing list - <dev@doris.apache.org>. Mail to <dev-subscribe@doris.apache.org>, follow the reply to subscribe the mail list.
* Slack channel - [Join the Slack](https://join.slack.com/t/apachedoriscommunity/shared_invite/zt-11jb8gesh-7IukzSrdea6mqoG0HB4gZg)
