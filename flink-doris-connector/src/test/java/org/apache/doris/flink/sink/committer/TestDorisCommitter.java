package org.apache.doris.flink.sink.committer;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.apache.doris.flink.sink.DorisCommittable;
import org.apache.doris.flink.sink.HttpEntityMock;
import org.apache.doris.flink.sink.OptionUtils;

import org.apache.doris.flink.sink.committer.DorisCommitter;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;


import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDorisCommitter {

    DorisCommitter dorisCommitter;
    DorisCommittable dorisCommittable;
    HttpEntityMock entityMock;
    @Before
    public void setUp() throws Exception{
        DorisOptions dorisOptions = OptionUtils.buildDorisOptions();
        DorisReadOptions readOptions = OptionUtils.buildDorisReadOptions();
        dorisCommittable = new DorisCommittable("127.0.0.1:8710", "test", 0);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        entityMock = new HttpEntityMock();
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        StatusLine normalLine = new BasicStatusLine(new ProtocolVersion("http", 1, 0), 200, "");
        when(httpClient.execute(any())).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(normalLine);
        when(httpResponse.getEntity()).thenReturn(entityMock);
        dorisCommitter = new DorisCommitter(dorisOptions, readOptions, 2, httpClient);
    }

    @Test
    public void testCommitted() throws Exception {
        String response = "{\n" +
                "\"status\": \"Fail\",\n" +
                "\"msg\": \"errCode = 2, detailMessage = transaction [2] is already visible, not pre-committed.\"\n" +
                "}";
        this.entityMock.setValue(response);
        dorisCommitter.commit(Collections.singletonList(dorisCommittable));

    }

    @Test(expected = DorisRuntimeException.class)
    public void testCommitAbort() throws Exception{
        String response = "{\n" +
                "\"status\": \"Fail\",\n" +
                "\"msg\": \"errCode = 2, detailMessage = transaction [25] is already aborted. abort reason: User Abort\"\n" +
                "}";
        this.entityMock.setValue(response);
        dorisCommitter.commit(Collections.singletonList(dorisCommittable));
    }
}
