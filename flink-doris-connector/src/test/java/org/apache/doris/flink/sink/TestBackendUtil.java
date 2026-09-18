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

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.models.BackendV2;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

public class TestBackendUtil {

    private MockedStatic<BackendUtil> backendUtilMockedStatic;

    @Before
    public void setUp() {
        backendUtilMockedStatic = mockStatic(BackendUtil.class);
        backendUtilMockedStatic
                .when(() -> BackendUtil.getInstance(any(), any(), any()))
                .thenCallRealMethod();
        backendUtilMockedStatic
                .when(() -> BackendUtil.tryHttpConnection(anyString()))
                .thenReturn(true);
    }

    @After
    public void tearDown() {
        backendUtilMockedStatic.close();
    }

    @Test
    public void testFeNodesAreFilteredOnlyDuringInitialization() {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030,127.0.0.2:8030,127.0.0.3:8030")
                        .setAutoRedirect(true)
                        .build();
        backendUtilMockedStatic
                .when(() -> BackendUtil.tryHttpConnection(anyString()))
                .thenReturn(false);
        backendUtilMockedStatic
                .when(() -> BackendUtil.tryHttpConnection("127.0.0.2:8030"))
                .thenReturn(true);
        backendUtilMockedStatic
                .when(() -> BackendUtil.tryHttpConnection("127.0.0.3:8030"))
                .thenReturn(true);

        BackendUtil backendUtil =
                BackendUtil.getInstance(
                        options, DorisReadOptions.defaults(), LoggerFactory.getLogger(getClass()));

        backendUtilMockedStatic
                .when(() -> BackendUtil.tryHttpConnection(anyString()))
                .thenReturn(false);
        List<String> selected =
                Arrays.asList(backendUtil.getAvailableBackend(), backendUtil.getAvailableBackend());
        Assert.assertTrue(selected.contains("127.0.0.2:8030"));
        Assert.assertTrue(selected.contains("127.0.0.3:8030"));
        Assert.assertEquals(selected.get(0), backendUtil.getAvailableBackend());

        // Reinitialization discovers the current availability instead of reusing the old list.
        backendUtilMockedStatic
                .when(() -> BackendUtil.tryHttpConnection("127.0.0.3:8030"))
                .thenReturn(true);
        BackendUtil recovered =
                BackendUtil.getInstance(
                        options, DorisReadOptions.defaults(), LoggerFactory.getLogger(getClass()));
        Assert.assertEquals("127.0.0.3:8030", recovered.getAvailableBackend());
    }

    @Test(expected = DorisRuntimeException.class)
    public void testNoAvailableFeFailsDuringInitialization() {
        backendUtilMockedStatic
                .when(() -> BackendUtil.tryHttpConnection(anyString()))
                .thenReturn(false);
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030,127.0.0.2:8030")
                        .setAutoRedirect(true)
                        .build();

        BackendUtil.getInstance(
                options, DorisReadOptions.defaults(), LoggerFactory.getLogger(getClass()));
    }

    @Test
    public void testGetAvailableBackend() throws Exception {
        List<BackendV2.BackendRowV2> backends =
                Arrays.asList(
                        newBackend("127.0.0.1", 8040),
                        newBackend("127.0.0.2", 8040),
                        newBackend("127.0.0.3", 8040));
        BackendUtil backendUtil = new BackendUtil(backends);
        Assert.assertEquals(backends.get(0).toBackendString(), backendUtil.getAvailableBackend());
        Assert.assertEquals(backends.get(1).toBackendString(), backendUtil.getAvailableBackend());
        Assert.assertEquals(backends.get(2).toBackendString(), backendUtil.getAvailableBackend());
        Assert.assertEquals(backends.get(0).toBackendString(), backendUtil.getAvailableBackend());
    }

    private BackendV2.BackendRowV2 newBackend(String host, int port) {
        BackendV2.BackendRowV2 backend = new BackendV2.BackendRowV2();
        backend.setIp(host);
        backend.setHttpPort(port);
        return backend;
    }
}
