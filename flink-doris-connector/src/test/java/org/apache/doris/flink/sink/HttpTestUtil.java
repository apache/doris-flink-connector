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

import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicStatusLine;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test Util for Http. */
public class HttpTestUtil {
    public static final String LABEL_EXIST_PRE_COMMIT_RESPONSE =
            "{\n"
                    + "\"TxnId\": 1,\n"
                    + "\"Label\": \"test001_0_1\",\n"
                    + "\"TwoPhaseCommit\": \"true\",\n"
                    + "\"Status\": \"Label Already Exists\",\n"
                    + "\"ExistingJobStatus\": \"PRECOMMITTED\",\n"
                    + "\"Message\": \"errCode = 2, detailMessage = Label [test001_0_1] has already been used, relate to txn [1]\",\n"
                    + "\"NumberTotalRows\": 0,\n"
                    + "\"NumberLoadedRows\": 0,\n"
                    + "\"NumberFilteredRows\": 0,\n"
                    + "\"NumberUnselectedRows\": 0,\n"
                    + "\"LoadBytes\": 0,\n"
                    + "\"LoadTimeMs\": 0,\n"
                    + "\"BeginTxnTimeMs\": 0,\n"
                    + "\"StreamLoadPutTimeMs\": 0,\n"
                    + "\"ReadDataTimeMs\": 0,\n"
                    + "\"WriteDataTimeMs\": 0,\n"
                    + "\"CommitAndPublishTimeMs\": 0\n"
                    + "}\n"
                    + "\n";

    public static final String LABEL_EXIST_PRE_COMMIT_TABLE_RESPONSE =
            "{\n"
                    + "\"TxnId\": 1,\n"
                    + "\"Label\": \"test001_db_table_0_1\",\n"
                    + "\"TwoPhaseCommit\": \"true\",\n"
                    + "\"Status\": \"Label Already Exists\",\n"
                    + "\"ExistingJobStatus\": \"PRECOMMITTED\",\n"
                    + "\"Message\": \"errCode = 2, detailMessage = Label [test001_db_table_0_1] has already been used, relate to txn [1]\",\n"
                    + "\"NumberTotalRows\": 0,\n"
                    + "\"NumberLoadedRows\": 0,\n"
                    + "\"NumberFilteredRows\": 0,\n"
                    + "\"NumberUnselectedRows\": 0,\n"
                    + "\"LoadBytes\": 0,\n"
                    + "\"LoadTimeMs\": 0,\n"
                    + "\"BeginTxnTimeMs\": 0,\n"
                    + "\"StreamLoadPutTimeMs\": 0,\n"
                    + "\"ReadDataTimeMs\": 0,\n"
                    + "\"WriteDataTimeMs\": 0,\n"
                    + "\"CommitAndPublishTimeMs\": 0\n"
                    + "}\n"
                    + "\n";

    public static final String PRE_COMMIT_RESPONSE =
            "{\n"
                    + "\"TxnId\": 2,\n"
                    + "\"Label\": \"test001_0_2\",\n"
                    + "\"TwoPhaseCommit\": \"true\",\n"
                    + "\"Status\": \"Success\",\n"
                    + "\"Message\": \"OK\",\n"
                    + "\"NumberTotalRows\": 0,\n"
                    + "\"NumberLoadedRows\": 0,\n"
                    + "\"NumberFilteredRows\": 0,\n"
                    + "\"NumberUnselectedRows\": 0,\n"
                    + "\"LoadBytes\": 0,\n"
                    + "\"LoadTimeMs\": 0,\n"
                    + "\"BeginTxnTimeMs\": 0,\n"
                    + "\"StreamLoadPutTimeMs\": 0,\n"
                    + "\"ReadDataTimeMs\": 0,\n"
                    + "\"WriteDataTimeMs\": 0,\n"
                    + "\"CommitAndPublishTimeMs\": 0\n"
                    + "}\n"
                    + "\n";

    public static final String PRE_COMMIT_TABLE_RESPONSE =
            "{\n"
                    + "\"TxnId\": 2,\n"
                    + "\"Label\": \"test001_db_table_0_2\",\n"
                    + "\"TwoPhaseCommit\": \"true\",\n"
                    + "\"Status\": \"Success\",\n"
                    + "\"Message\": \"OK\",\n"
                    + "\"NumberTotalRows\": 0,\n"
                    + "\"NumberLoadedRows\": 0,\n"
                    + "\"NumberFilteredRows\": 0,\n"
                    + "\"NumberUnselectedRows\": 0,\n"
                    + "\"LoadBytes\": 0,\n"
                    + "\"LoadTimeMs\": 0,\n"
                    + "\"BeginTxnTimeMs\": 0,\n"
                    + "\"StreamLoadPutTimeMs\": 0,\n"
                    + "\"ReadDataTimeMs\": 0,\n"
                    + "\"WriteDataTimeMs\": 0,\n"
                    + "\"CommitAndPublishTimeMs\": 0\n"
                    + "}\n"
                    + "\n";

    public static final String ABORT_SUCCESS_RESPONSE =
            "{\n"
                    + "\"status\": \"Success\",\n"
                    + "\"msg\": \"transaction [1] abort successfully.\"\n"
                    + "\n"
                    + "}";

    public static final String ABORT_FAILED_RESPONSE =
            "{\n"
                    + "    \"status\": \"Internal error\",\n"
                    + "    \"msg\": \"transaction operation should be 'commit' or 'abort'\"\n"
                    + "}";

    public static StatusLine normalLine =
            new BasicStatusLine(new ProtocolVersion("http", 1, 0), 200, "");
    public static StatusLine abnormalLine =
            new BasicStatusLine(new ProtocolVersion("http", 1, 0), 404, "");

    public static CloseableHttpResponse getResponse(String response, boolean ok) {
        HttpEntityMock httpEntityMock = new HttpEntityMock();
        httpEntityMock.setValue(response);
        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
        if (ok) {
            when(httpResponse.getStatusLine()).thenReturn(normalLine);
        } else {
            when(httpResponse.getStatusLine()).thenReturn(abnormalLine);
        }
        when(httpResponse.getEntity()).thenReturn(httpEntityMock);
        return httpResponse;
    }
}
