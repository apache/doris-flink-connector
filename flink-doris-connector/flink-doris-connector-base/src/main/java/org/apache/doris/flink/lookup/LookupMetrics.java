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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.io.Serializable;

/** lookup join metrics. */
public class LookupMetrics implements Serializable {
    public static final String HIT_COUNT = "hitCount";
    public static final String MISS_COUNT = "missCount";
    public static final String LOAD_COUNT = "loadCount";
    private transient Counter hitCounter;
    private transient Counter missCounter;
    private transient Counter loadCounter;

    public LookupMetrics(MetricGroup metricGroup) {
        hitCounter = metricGroup.counter(HIT_COUNT);
        missCounter = metricGroup.counter(MISS_COUNT);
        loadCounter = metricGroup.counter(LOAD_COUNT);
    }

    public void incHitCount() {
        hitCounter.inc();
    }

    public void incMissCount() {
        missCounter.inc();
    }

    public void incLoadCount() {
        loadCounter.inc();
    }
}
