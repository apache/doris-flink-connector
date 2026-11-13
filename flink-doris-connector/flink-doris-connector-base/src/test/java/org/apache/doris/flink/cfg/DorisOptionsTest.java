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

package org.apache.doris.flink.cfg;

import org.apache.flink.util.InstantiationUtil;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class DorisOptionsTest {

    @Test
    public void testMissingTlsOptionsFallsBackToDisabled() throws Exception {
        DorisConnectionOptions options =
                new DorisConnectionOptions("127.0.0.1:8030", "root", "password");
        Field tlsOptionsField = DorisConnectionOptions.class.getDeclaredField("tlsOptions");
        tlsOptionsField.setAccessible(true);
        tlsOptionsField.set(options, null);

        byte[] serialized = InstantiationUtil.serializeObject(options);
        DorisConnectionOptions restored =
                InstantiationUtil.deserializeObject(
                        serialized, Thread.currentThread().getContextClassLoader());

        Assert.assertNotNull(restored.getTlsOptions());
        Assert.assertFalse(restored.getTlsOptions().isEnabled());
    }

    @Test
    public void testEquals() {
        DorisOptions.Builder options =
                DorisOptions.builder()
                        .setFenodes("fenodes")
                        .setTableIdentifier("db.table")
                        .setUsername("user")
                        .setPassword("password")
                        .setBenodes("benodes")
                        .setAutoRedirect(true)
                        .setJdbcUrl("xxx")
                        .setTlsOptions(
                                DorisTlsOptions.builder()
                                        .setEnabled(true)
                                        .setCaCertificatePath("certs/ca.pem")
                                        .build());
        DorisOptions.Builder options1 =
                DorisOptions.builder()
                        .setFenodes("fenodes")
                        .setTableIdentifier("db.table")
                        .setUsername("user")
                        .setPassword("password")
                        .setBenodes("benodes")
                        .setAutoRedirect(true)
                        .setJdbcUrl("xxx")
                        .setTlsOptions(
                                DorisTlsOptions.builder()
                                        .setEnabled(true)
                                        .setCaCertificatePath("certs/ca.pem")
                                        .build());
        DorisOptions exceptedOption = options.build();
        Assert.assertNotEquals(exceptedOption, null);
        Assert.assertEquals(exceptedOption, options.build());
        Assert.assertEquals(exceptedOption, options1.build());

        options1.setFenodes("127.0.0.1:8030");
        Assert.assertNotEquals(exceptedOption, options1.build());

        options1.setFenodes("fenodes");
        options1.setTableIdentifier("xxx");
        Assert.assertNotEquals(exceptedOption, options1.build());

        options1.setTableIdentifier("db.table");
        options1.setUsername("xxx");
        Assert.assertNotEquals(exceptedOption, options1.build());

        options1.setUsername("user");
        options1.setPassword("change");
        Assert.assertNotEquals(exceptedOption, options1.build());

        options1.setPassword("password");
        options1.setBenodes("xxx");
        Assert.assertNotEquals(exceptedOption, options1.build());

        options1.setBenodes("benodes");
        options1.setAutoRedirect(false);
        Assert.assertNotEquals(exceptedOption, options1.build());

        options1.setAutoRedirect(true);
        options1.setJdbcUrl("xxx1");
        Assert.assertNotEquals(exceptedOption, options1.build());

        options1.setJdbcUrl("xxx");
        options1.setTlsOptions(DorisTlsOptions.disabled());
        Assert.assertNotEquals(exceptedOption, options1.build());
    }
}
