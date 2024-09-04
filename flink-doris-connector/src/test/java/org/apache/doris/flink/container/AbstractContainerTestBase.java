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

package org.apache.doris.flink.container;

import org.apache.doris.flink.container.instance.ContainerService;
import org.apache.doris.flink.container.instance.DorisContainer;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Objects;

public abstract class AbstractContainerTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractContainerTestBase.class);
    private static ContainerService dorisContainerService;

    @BeforeClass
    public static void initContainers() {
        LOG.info("Trying to start doris containers.");
        initDorisContainer();
    }

    private static void initDorisContainer() {
        if (Objects.nonNull(dorisContainerService) && dorisContainerService.isRunning()) {
            LOG.info("The doris container has been started and is running status.");
            return;
        }
        dorisContainerService = new DorisContainer();
        dorisContainerService.startContainer();
        LOG.info("Doris container was started.");
    }

    protected static Connection getDorisQueryConnection() {
        return dorisContainerService.getQueryConnection();
    }

    protected String getFenodes() {
        return dorisContainerService.getFenodes();
    }

    protected String getBenodes() {
        return dorisContainerService.getBenodes();
    }

    protected String getDorisUsername() {
        return dorisContainerService.getUsername();
    }

    protected String getDorisPassword() {
        return dorisContainerService.getPassword();
    }

    protected String getDorisQueryUrl() {
        return String.format(
                "jdbc:mysql://%s:%s",
                getDorisInstanceHost(), dorisContainerService.getMappedPort(9030));
    }

    protected String getDorisInstanceHost() {
        return dorisContainerService.getInstanceHost();
    }

    public static void closeContainers() {
        LOG.info("Starting to close containers.");
        closeDorisContainer();
    }

    private static void closeDorisContainer() {
        if (Objects.isNull(dorisContainerService)) {
            return;
        }
        dorisContainerService.close();
        LOG.info("Doris container was closed.");
    }
}
