package org.apache.doris.flink.sink;

import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicStatusLine;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpTestUtil {
    public static final String LABEL_EXIST_PRE_COMMIT_RESPONSE = "{\n" +
            "\"TxnId\": 1,\n" +
            "\"Label\": \"test001_0_1\",\n" +
            "\"TwoPhaseCommit\": \"true\",\n" +
            "\"Status\": \"Label Already Exists\",\n" +
            "\"ExistingJobStatus\": \"PRECOMMITTED\",\n" +
            "\"Message\": \"errCode = 2, detailMessage = Label [test001_0_1] has already been used, relate to txn [1]\",\n" +
            "\"NumberTotalRows\": 0,\n" +
            "\"NumberLoadedRows\": 0,\n" +
            "\"NumberFilteredRows\": 0,\n" +
            "\"NumberUnselectedRows\": 0,\n" +
            "\"LoadBytes\": 0,\n" +
            "\"LoadTimeMs\": 0,\n" +
            "\"BeginTxnTimeMs\": 0,\n" +
            "\"StreamLoadPutTimeMs\": 0,\n" +
            "\"ReadDataTimeMs\": 0,\n" +
            "\"WriteDataTimeMs\": 0,\n" +
            "\"CommitAndPublishTimeMs\": 0\n" +
            "}\n" +
            "\n";
    public static final String PRE_COMMIT_RESPONSE = "{\n" +
            "\"TxnId\": 2,\n" +
            "\"Label\": \"test001_0_2\",\n" +
            "\"TwoPhaseCommit\": \"true\",\n" +
            "\"Status\": \"Success\",\n" +
            "\"Message\": \"OK\",\n" +
            "\"NumberTotalRows\": 0,\n" +
            "\"NumberLoadedRows\": 0,\n" +
            "\"NumberFilteredRows\": 0,\n" +
            "\"NumberUnselectedRows\": 0,\n" +
            "\"LoadBytes\": 0,\n" +
            "\"LoadTimeMs\": 0,\n" +
            "\"BeginTxnTimeMs\": 0,\n" +
            "\"StreamLoadPutTimeMs\": 0,\n" +
            "\"ReadDataTimeMs\": 0,\n" +
            "\"WriteDataTimeMs\": 0,\n" +
            "\"CommitAndPublishTimeMs\": 0\n" +
            "}\n" +
            "\n";

    public static final String ABORT_SUCCESS_RESPONSE = "{\n" +
            "\"status\": \"Success\",\n" +
            "\"msg\": \"transaction [1] abort successfully.\"\n" +
            "\n" +
            "}";

    public static StatusLine normalLine = new BasicStatusLine(new ProtocolVersion("http", 1, 0), 200, "");
    public static StatusLine abnormalLine = new BasicStatusLine(new ProtocolVersion("http", 1, 0), 404, "");

    public static CloseableHttpResponse getResponse(String response, boolean ok) {
        HttpEntityMock httpEntityMock = new HttpEntityMock();
        httpEntityMock.setValue(response);
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        if(ok) {
            when(httpResponse.getStatusLine()).thenReturn(normalLine);
        } else {
            when(httpResponse.getStatusLine()).thenReturn(abnormalLine);
        }
        when(httpResponse.getEntity()).thenReturn(httpEntityMock);
        return httpResponse;
    }
}
