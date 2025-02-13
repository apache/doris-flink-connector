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

package org.apache.doris.flink.lookup;

import java.lang.reflect.Array;

public class RecordKey {
    Record record;
    int[] keys;

    public RecordKey(Record record) {
        this.record = record;
        keys = record.getKeyIndex();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RecordKey recordKey = (RecordKey) o;
        if (record == recordKey.record) {
            return true;
        }

        if (record.getKeyIndex().length != recordKey.record.getKeyIndex().length) {
            return false;
        }
        if (record.getKeyIndex().length == 0) {
            return false;
        }
        for (int i : record.getKeyIndex()) {
            Object left = record.getObject(i);
            Object right = recordKey.record.getObject(i);
            if (!equals(left, right)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hash(record, keys);
    }

    public static boolean equals(Object obj0, Object obj1) {
        if (obj0 == null) {
            if (obj1 != null) {
                return false;
            } else {
                return true;
            }
        } else {
            if (obj1 == null) {
                return false;
            }
        }

        if (obj0.getClass().isArray()) {
            if (obj1.getClass().isArray()) {
                int length0 = Array.getLength(obj0);
                int length1 = Array.getLength(obj1);
                if (length0 != length1) {
                    return false;
                } else {
                    for (int i = 0; i < length0; ++i) {
                        Object child0 = Array.get(obj0, i);
                        Object child1 = Array.get(obj1, i);
                        if (!equals(child0, child1)) {
                            return false;
                        }
                    }
                }
            } else {
                return false;
            }
        } else {
            if (obj1.getClass().isArray()) {
                return false;
            } else {
                return obj0.equals(obj1);
            }
        }
        return true;
    }

    public static int hash(Record record, int[] indexes) {
        int hash = 0;
        boolean first = true;
        for (int i : indexes) {
            if (first) {
                hash = hashCode(record.getObject(i));
            } else {
                hash ^= hashCode(record.getObject(i));
            }
            first = false;
        }
        return hash;
    }

    public static int hashCode(Object obj) {
        if (obj == null) {
            return 0;
        }
        if (obj.getClass().isArray()) {
            int hash = 0;
            int length = Array.getLength(obj);
            for (int i = 0; i < length; ++i) {
                Object child = Array.get(obj, i);
                hash = hash * 31 + (child == null ? 0 : child.hashCode());
            }
            return hash;
        } else {
            return obj.hashCode();
        }
    }
}
