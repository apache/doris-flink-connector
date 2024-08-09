package org.apache.doris.flink.autoci.e2e;

import org.apache.doris.flink.autoci.AbstractAutoCITestBase;
import org.apache.doris.flink.autoci.container.ContainerService;
import org.apache.doris.flink.autoci.container.MySQLContainerService;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.tools.cdc.CdcTools;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AbstractE2EService extends AbstractAutoCITestBase {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractE2EService.class);
    private static ContainerService mysqlContainerService;
    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @BeforeClass
    public static void initE2EContainers() {
        LOG.info("Starting to init E2E containers.");
        initMySQLContainer();
    }

    private static void initMySQLContainer() {
        if (Objects.nonNull(mysqlContainerService)) {
            return;
        }
        mysqlContainerService = new MySQLContainerService();
        mysqlContainerService.startContainer();
        LOG.info("Mysql container was started.");
    }

    protected String getMySQLInstanceHost() {
        return mysqlContainerService.getInstanceHost();
    }

    protected Integer getMySQLQueryPort() {
        return mysqlContainerService.getMappedPort(3306);
    }

    protected String getMySQLUsername() {
        return mysqlContainerService.getUsername();
    }

    protected String getMySQLPassword() {
        return mysqlContainerService.getPassword();
    }

    protected Connection getMySQLQueryConnection() {
        return mysqlContainerService.getQueryConnection();
    }

    protected void submitE2EJob(String[] args) {
        executorService.submit(
                () -> {
                    try {
                        CdcTools.main(args);
                        LOG.info("E2E task has been submitted to start.");
                        Thread.sleep(10000);
                    } catch (Exception e) {
                        LOG.warn("Failed to submit E2E job.", e);
                        throw new DorisRuntimeException(e);
                    }
                });
    }

    private static void shutdownE2EJob() {
        try {
            LOG.info("Shutting down E2E executorService.");
            executorService.shutdown();
            if (!executorService.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
                    LOG.error("E2E executorService did not terminate.");
                }
            }
        } catch (InterruptedException ie) {
            LOG.info("E2E executorService will shut down now.");
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @AfterClass
    public static void closeE2EContainers() {
        LOG.info("Starting to close E2E containers.");
        closeMySQLContainer();
    }

    private static void closeMySQLContainer() {
        if (Objects.isNull(mysqlContainerService)) {
            return;
        }
        shutdownE2EJob();
        mysqlContainerService.close();
        LOG.info("Mysql container was closed.");
    }
}
