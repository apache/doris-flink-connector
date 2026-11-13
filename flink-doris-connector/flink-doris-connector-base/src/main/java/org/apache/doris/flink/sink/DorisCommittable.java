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

package org.apache.doris.flink.sink;

import java.util.Objects;

/** DorisCommittable hold the info for Committer to commit. */
public class DorisCommittable implements DorisAbstractCommittable {
    private final String hostPort;
    private final String db;
    private final long txnID;

    public DorisCommittable(String hostPort, String db, long txnID) {
        this.hostPort = hostPort;
        this.db = db;
        this.txnID = txnID;
    }

    public String getHostPort() {
        return hostPort;
    }

    public String getDb() {
        return db;
    }

    public long getTxnID() {
        return txnID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DorisCommittable that = (DorisCommittable) o;
        return txnID == that.txnID
                && Objects.equals(hostPort, that.hostPort)
                && Objects.equals(db, that.db);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostPort, db, txnID);
    }

    @Override
    public String toString() {
        return "DorisCommittable{"
                + "hostPort='"
                + hostPort
                + '\''
                + ", db='"
                + db
                + '\''
                + ", txnID="
                + txnID
                + '}';
    }
}
