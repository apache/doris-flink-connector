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

Flink Doris Connector now support flink version from 1.11 to 1.15.

If you wish to contribute or use a connector from flink 1.13 (and earlier), please use the [branch-for-flink-before-1.13](https://github.com/apache/doris-flink-connector/tree/branch-for-flink-before-1.13)

More information about compilation and usage, please visit [Flink Doris Connector](https://doris.apache.org/docs/ecosystem/flink-doris-connector/)

## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

## Building
You need to copy customer_env.sh.tpl to customer_env.sh before build and you need to configure it before build.
```shell
sh build.sh --flink 1.15.2 --scala 2.12
```

If you are using mac, you must install gnu-getopt before executing the build.sh.
```shell
brew install gnu-getopt
brew link --force gnu-getopt
```


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
