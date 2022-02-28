package org.apache.doris.flink.sink.writer;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpEntityMock;
import org.apache.doris.flink.sink.HttpTestUtil;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.doris.flink.sink.committer.DorisCommitter;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.tools.cmd.Opt;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDorisStreamLoad {

    DorisOptions dorisOptions;
    DorisReadOptions readOptions;
    DorisExecutionOptions executionOptions;
    @Before
    public void setUp() throws Exception{
        dorisOptions = OptionUtils.buildDorisOptions();
        readOptions = OptionUtils.buildDorisReadOptions();
        executionOptions = OptionUtils.buildExecutionOptional();
    }

    @Test
    public void testAbortPreCommit() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse existLabelResponse = HttpTestUtil.getResponse(HttpTestUtil.LABEL_EXIST_PRE_COMMIT_RESPONSE, true);
        CloseableHttpResponse abortSuccessResponse = HttpTestUtil.getResponse(HttpTestUtil.ABORT_SUCCESS_RESPONSE, true);
        CloseableHttpResponse preCommitResponse = HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(existLabelResponse,abortSuccessResponse,preCommitResponse );
        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad("", dorisOptions, executionOptions, "", httpClient);
        dorisStreamLoad.abortPreCommit("test001_0", 1);
    }

    @Test
    public void testWriteOneRecordInCsv() throws Exception{
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse = HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);
        byte[] writeBuffer = "test".getBytes(StandardCharsets.UTF_8);
        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad("", dorisOptions, executionOptions, "", httpClient);
        dorisStreamLoad.startLoad(1);
        dorisStreamLoad.writeRecord(writeBuffer);
        dorisStreamLoad.stopLoad();
        byte[] buff = new byte[4];
        int n = dorisStreamLoad.getRecordStream().read(buff);
        dorisStreamLoad.getRecordStream().read(new byte[4]);

        Assert.assertEquals(4, n);
        Assert.assertArrayEquals(writeBuffer, buff);
    }

    @Test
    public void testWriteTwoRecordInCsv() throws Exception{
        executionOptions = OptionUtils.buildExecutionOptional();
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse = HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);
        byte[] writeBuffer = "test".getBytes(StandardCharsets.UTF_8);
        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad("", dorisOptions, executionOptions, "", httpClient);
        dorisStreamLoad.startLoad(1);
        dorisStreamLoad.writeRecord(writeBuffer);
        dorisStreamLoad.writeRecord(writeBuffer);
        dorisStreamLoad.stopLoad();
        byte[] buff = new byte[9];
        int n = dorisStreamLoad.getRecordStream().read(buff);
        int ret = dorisStreamLoad.getRecordStream().read(new byte[9]);
        Assert.assertEquals(-1, ret);
        Assert.assertEquals(9, n);
        Assert.assertArrayEquals("test\ntest".getBytes(StandardCharsets.UTF_8), buff);
    }

    @Test
    public void testWriteTwoRecordInJson() throws Exception{
        Properties properties = new Properties();
        properties.setProperty("column_separator", "|");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "json");
        executionOptions = OptionUtils.buildExecutionOptional(properties);
        byte[] expectBuffer = "[{\"id\": 1},{\"id\": 2}]".getBytes(StandardCharsets.UTF_8);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse = HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);

        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad("", dorisOptions, executionOptions, "", httpClient);
        dorisStreamLoad.startLoad(1);
        dorisStreamLoad.writeRecord("{\"id\": 1}".getBytes(StandardCharsets.UTF_8));
        dorisStreamLoad.writeRecord("{\"id\": 2}".getBytes(StandardCharsets.UTF_8));
        dorisStreamLoad.stopLoad();
        byte[] buff = new byte[expectBuffer.length];
        int n = dorisStreamLoad.getRecordStream().read(buff);

        int ret = dorisStreamLoad.getRecordStream().read(new byte[4]);
        Assert.assertEquals(-1, ret);
        Assert.assertEquals(expectBuffer.length, n);
        Assert.assertArrayEquals(expectBuffer, buff);
    }
}
