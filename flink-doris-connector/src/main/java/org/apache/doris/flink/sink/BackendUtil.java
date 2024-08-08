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

import org.apache.flink.annotation.VisibleForTesting;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.rest.models.BackendV2;
import org.apache.doris.flink.rest.models.BackendV2.BackendRowV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BackendUtil {
    private static final Logger LOG = LoggerFactory.getLogger(BackendUtil.class);
    private final List<BackendV2.BackendRowV2> backends;
    private long pos;

    public BackendUtil(List<BackendV2.BackendRowV2> backends) {
        this.backends = backends;
        this.pos = 0;
    }

    public BackendUtil(String beNodes) {
        this.backends = initBackends(beNodes);
        this.pos = 0;
    }

    private List<BackendV2.BackendRowV2> initBackends(String beNodes) {
        List<BackendV2.BackendRowV2> backends = new ArrayList<>();
        List<String> nodes = Arrays.asList(beNodes.split(","));
        nodes.forEach(
                node -> {
                    if (tryHttpConnection(node)) {
                        LOG.info("{} backend http connection success.", node);
                        node = node.trim();
                        String[] ipAndPort = node.split(":");
                        BackendRowV2 backendRowV2 = new BackendRowV2();
                        backendRowV2.setIp(ipAndPort[0]);
                        backendRowV2.setHttpPort(Integer.parseInt(ipAndPort[1]));
                        backendRowV2.setAlive(true);
                        backends.add(backendRowV2);
                    }
                });
        return backends;
    }

    public static BackendUtil getInstance(
            DorisOptions dorisOptions, DorisReadOptions readOptions, Logger logger) {
        if (StringUtils.isNotEmpty(dorisOptions.getBenodes())) {
            return new BackendUtil(dorisOptions.getBenodes());
        } else {
            return new BackendUtil(RestService.getBackendsV2(dorisOptions, readOptions, logger));
        }
    }

    public String getAvailableBackend() {
        return getAvailableBackend(0);
    }

    public String getAvailableBackend(int subtaskId) {
        long tmp = pos + backends.size();
        while (pos < tmp) {
            BackendV2.BackendRowV2 backend =
                    backends.get((int) ((pos + subtaskId) % backends.size()));
            pos++;
            String res = backend.toBackendString();
            if (tryHttpConnection(res)) {
                return res;
            }
        }
        throw new DorisRuntimeException("no available backend.");
    }

    public static boolean tryHttpConnection(String host) {
        try {
            LOG.debug("try to connect host {}", host);
            host = "http://" + host;
            URL url = new URL(host);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(60000);
            connection.setReadTimeout(60000);
            int responseCode = connection.getResponseCode();
            String responseMessage = connection.getResponseMessage();
            connection.disconnect();
            if (200 == responseCode) {
                return true;
            }
            LOG.warn(
                    "Failed to connect host {}, responseCode={}, msg={}",
                    host,
                    responseCode,
                    responseMessage);
            return false;
        } catch (Exception ex) {
            LOG.warn("Failed to connect to host:{}", host, ex);
            return false;
        }
    }

    @VisibleForTesting
    public List<BackendRowV2> getBackends() {
        return backends;
    }
}
