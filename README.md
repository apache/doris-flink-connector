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

Flink Doris Connector now support flink version from 1.11 to 1.20.

If you wish to contribute or use a connector from flink 1.13 (and earlier), please use the [branch-for-flink-before-1.13](https://github.com/apache/doris-flink-connector/tree/branch-for-flink-before-1.13)

More information about compilation and usage, please visit [Flink Doris Connector](https://doris.apache.org/docs/dev/ecosystem/flink-doris-connector)

## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

## How to Build

You need to copy customer_env.sh.tpl to customer_env.sh before build and you need to configure it before build.
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
