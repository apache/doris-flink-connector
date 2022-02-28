package org.apache.doris.flink.sink.writer;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpTestUtil;
import org.apache.doris.flink.sink.OptionUtils;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDorisWriter {
    DorisOptions dorisOptions;
    DorisReadOptions readOptions;
    DorisExecutionOptions executionOptions;

    /**
     *  loading = false;
     *         RespContent respContent = dorisStreamLoad.stopLoad();
     *         if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
     *             String errMsg = String.format("stream load error: %s, see more in %s", respContent.getMessage(), respContent.getErrorURL());
     *             throw new DorisRuntimeException(errMsg);
     *         }
     *
     *         long txnId = respContent.getTxnId();
     *
     *         return ImmutableList.of(new DorisCommittable(dorisStreamLoad.getHostPort(), dorisStreamLoad.getDb(), txnId));
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception{
        dorisOptions = OptionUtils.buildDorisOptions();
        readOptions = OptionUtils.buildDorisReadOptions();
        executionOptions = OptionUtils.buildExecutionOptional();
    }
    @Test
    public void testPrepareCommit() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse = HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);

        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad("local:8040", dorisOptions, executionOptions, "", httpClient);
        dorisStreamLoad.startLoad(1);
        Sink.InitContext initContext = mock(Sink.InitContext.class);
        when(initContext.getRestoredCheckpointId()).thenReturn(OptionalLong.of(1));
        DorisWriter<String> dorisWriter = new DorisWriter<String>(initContext, Collections.emptyList(),new SimpleStringSerializer(), dorisOptions, readOptions, executionOptions);
        dorisWriter.setDorisStreamLoad(dorisStreamLoad);
        List<DorisCommittable> committableList = dorisWriter.prepareCommit(true);

        Assert.assertEquals(1, committableList.size());
        Assert.assertEquals("local:8040", committableList.get(0).getHostPort());
        Assert.assertEquals("db_test", committableList.get(0).getDb());
        Assert.assertEquals(2, committableList.get(0).getTxnID());
        Assert.assertFalse(dorisWriter.isLoading());
    }

    @Test
    public void testSnapshot() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse preCommitResponse = HttpTestUtil.getResponse(HttpTestUtil.PRE_COMMIT_RESPONSE, true);
        when(httpClient.execute(any())).thenReturn(preCommitResponse);

        DorisStreamLoad dorisStreamLoad = new DorisStreamLoad("local:8040", dorisOptions, executionOptions, "", httpClient);
        Sink.InitContext initContext = mock(Sink.InitContext.class);
        when(initContext.getRestoredCheckpointId()).thenReturn(OptionalLong.of(1));
        DorisWriter<String> dorisWriter = new DorisWriter<String>(initContext, Collections.emptyList(),new SimpleStringSerializer(), dorisOptions, readOptions, executionOptions);
        dorisWriter.setDorisStreamLoad(dorisStreamLoad);
        List<DorisWriterState> writerStates = dorisWriter.snapshotState(1);

        Assert.assertEquals(1, writerStates.size());
        Assert.assertEquals("doris", writerStates.get(0).getLabelPrefix());
        Assert.assertTrue(dorisWriter.isLoading());
    }
}
