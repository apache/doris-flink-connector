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

package org.apache.doris.flink.sink.writer.tvf;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class S3ClientObjectStoreTest {

    @Test
    public void testPutObject() throws Exception {
        S3Client s3Client = mock(S3Client.class);
        S3ClientObjectStore objectStore = new S3ClientObjectStore(s3Client, "bucket");
        byte[] content = "{\"id\":1}\n".getBytes(StandardCharsets.UTF_8);

        objectStore.put("prefix_tbl_0_1_0.json", content);

        ArgumentCaptor<PutObjectRequest> requestCaptor =
                ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3Client).putObject(requestCaptor.capture(), bodyCaptor.capture());
        Assert.assertEquals("bucket", requestCaptor.getValue().bucket());
        Assert.assertEquals("prefix_tbl_0_1_0.json", requestCaptor.getValue().key());
        Assert.assertEquals("application/x-ndjson", requestCaptor.getValue().contentType());
        try (InputStream input = bodyCaptor.getValue().contentStreamProvider().newStream()) {
            byte[] actual = new byte[content.length];
            Assert.assertEquals(content.length, input.read(actual));
            Assert.assertArrayEquals(content, actual);
        }

        objectStore.close();
        verify(s3Client).close();
    }
}
