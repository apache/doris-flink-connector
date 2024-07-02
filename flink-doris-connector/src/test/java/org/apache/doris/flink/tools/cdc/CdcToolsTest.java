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

package org.apache.doris.flink.tools.cdc;

import org.apache.flink.api.java.utils.MultipleParameterTool;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CdcToolsTest {

    @Test
    public void getConfigMapTest() {
        MultipleParameterTool params =
                MultipleParameterTool.fromArgs(
                        new String[] {
                            "--sink-conf", "fenodes=127.0.0.1:8030", "--sink-conf", "password="
                        });
        Map<String, String> sinkConf = CdcTools.getConfigMap(params, "sink-conf");

        Map<String, String> excepted = new HashMap<>();
        excepted.put("password", "");
        excepted.put("fenodes", "127.0.0.1:8030");
        Assert.assertEquals(sinkConf, excepted);

        Map<String, String> mysqlConf = CdcTools.getConfigMap(params, "--mysql-conf");
        Assert.assertNull(mysqlConf);

        MultipleParameterTool params2 =
                MultipleParameterTool.fromArgs(new String[] {"--sink-conf", "fenodes"});
        Map<String, String> sinkConf2 = CdcTools.getConfigMap(params2, "sink-conf");
        Assert.assertNull(sinkConf2);
    }
}
