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
package org.apache.doris.flink.sink.batch;


import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.apache.doris.flink.table.DorisConfigOptions;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.doris.flink.table.DorisConfigOptions.getDefaultParameterJdbcUrl;

//
public class TestDefaultParameter {

    @Test
    public void testParameterMap() {
        //  Test map
        LinkedHashMap<String, String> jdbcUrlDefaultParameterMap = DorisConfigOptions.getJdbcUrlDefaultParameter();
        Assert.assertEquals(jdbcUrlDefaultParameterMap.size(), 1);
        String value = jdbcUrlDefaultParameterMap.get("rewriteBatchedStatements");
        Assert.assertEquals(value, "true");
    }

    @Test
    public void testParameterJdbcUrl() {
        LinkedHashMap<String, String> jdbcUrlDefaultParameterMap = DorisConfigOptions.getJdbcUrlDefaultParameter();
        //  Test map
        ArrayList<String> urlArrays = new ArrayList<>();
        Assert.assertEquals(getDefaultParameterJdbcUrl(jdbcUrlDefaultParameterMap, "jdbc:mysql://localhost:3306"), "jdbc:mysql://localhost:3306?rewriteBatchedStatements=true");
        Assert.assertEquals(getDefaultParameterJdbcUrl(jdbcUrlDefaultParameterMap, "jdbc:mysql://localhost:3306/test"), "jdbc:mysql://localhost:3306?rewriteBatchedStatements=true");
        Assert.assertEquals(getDefaultParameterJdbcUrl(jdbcUrlDefaultParameterMap, "jdbc:mysql://localhost:3306/test/"), "jdbc:mysql://localhost:3306?rewriteBatchedStatements=true");
        Assert.assertEquals(getDefaultParameterJdbcUrl(jdbcUrlDefaultParameterMap, "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"), "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true");
        Assert.assertEquals(getDefaultParameterJdbcUrl(jdbcUrlDefaultParameterMap, "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=false"), "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=false");
        Assert.assertEquals(getDefaultParameterJdbcUrl(jdbcUrlDefaultParameterMap, "jdbc:mysql://localhost:3306/test?useUnicode=true"), "jdbc:mysql://localhost:3306/test?useUnicode=true&rewriteBatchedStatements=true");
    }

}
