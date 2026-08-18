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

import org.apache.doris.flink.cfg.S3TvfOptions;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.net.URI;

/** S3 SDK based object store used by the TVF writer. */
public class S3ClientObjectStore implements S3ObjectStore {

    private static final String JSON_LINES_CONTENT_TYPE = "application/x-ndjson";

    private final S3Client s3Client;
    private final String bucket;

    public S3ClientObjectStore(S3TvfOptions options) {
        this(createClient(options), options.getBucket());
    }

    S3ClientObjectStore(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    private static S3Client createClient(S3TvfOptions options) {
        return S3Client.builder()
                .endpointOverride(URI.create(options.getEndpoint()))
                .region(Region.of(options.getRegion()))
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(
                                        options.getAccessKey(), options.getSecretKey())))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .serviceConfiguration(
                        S3Configuration.builder()
                                .pathStyleAccessEnabled(options.isPathStyleAccess())
                                .build())
                .build();
    }

    @Override
    public void put(String objectKey, byte[] content) throws IOException {
        PutObjectRequest request =
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectKey)
                        .contentType(JSON_LINES_CONTENT_TYPE)
                        .build();
        try {
            s3Client.putObject(request, RequestBody.fromBytes(content));
        } catch (RuntimeException e) {
            throw new IOException(
                    String.format(
                            "Failed to upload object '%s' to bucket '%s'.", objectKey, bucket),
                    e);
        }
    }

    @Override
    public void close() {
        s3Client.close();
    }
}
