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

# Startup from an external offset

When `source.binlog.offset-table` and `source.binlog.consumer-id` are configured,
a new job reads `offset_timestamp` for that consumer once through JDBC before
planning any snapshot. The offset table account needs SELECT and INSERT
permissions. Keep the consumer ID stable for the same source and consumption
configuration, and do not run concurrent jobs with the same consumer ID.

Startup precedence is:

1. If Flink restores Source checkpoint/savepoint state, use that state without
   reading the external offset. A restoration failure is an error.
2. Without restored Source state, a saved external offset takes precedence in
   every incremental mode, including `from-timestamp`. Start directly in the
   incremental phase at the saved boundary; `initial` skips snapshot planning.
3. With no record for the consumer, `initial` runs its first snapshot and
   `from-timestamp` uses `source.scan.timestamp`. `latest` with an offset table
   fails and requires `initial` or `from-timestamp` for its first start.
4. Without an offset table, the existing startup behavior applies.

A record with NULL, an invalid timestamp, or multiple rows for the same consumer
is an error, as are connection and SELECT failures. Timestamps use exactly
`yyyy-MM-dd HH:mm:ss`, with a valid calendar date. The saved boundary is used
unchanged: after a completed `[A, B)` window, the next window starts at `B`.

With external persistence enabled, `source.scan.timestamp` is a first-start
fallback. Changing it does not force replay when a saved offset exists. Use a
new consumer ID to replay from a chosen timestamp independently of the original
consumer.

Enable Flink checkpoints during execution to publish external offsets. The
published offset covers a completed checkpoint and may lag current processing.
Restarting a new job from it can replay changes already seen downstream. This only restores Source progress, not Flink operator state
or Sink transactions. It does not provide exactly-once recovery for stateful
computations or two-phase-commit sinks. Unconsumed history must still be retained
in Doris; binlog cleanup and dropped/replaced partitions can leave unrecoverable
gaps.
