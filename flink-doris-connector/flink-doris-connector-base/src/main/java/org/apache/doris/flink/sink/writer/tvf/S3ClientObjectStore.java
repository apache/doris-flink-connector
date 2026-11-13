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
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

/** S3 SDK based object store used by the TVF writer. */
public class S3ClientObjectStore implements S3ObjectStore {

    private static final String JSON_LINES_CONTENT_TYPE = "application/x-ndjson";
    private static final String ROLE_SESSION_NAME = "doris-flink-connector";

    private final S3Client s3Client;
    private final String bucket;
    private DefaultCredentialsProvider defaultCredentialsProvider;
    private StsClient stsClient;
    private StsAssumeRoleCredentialsProvider assumeRoleCredentialsProvider;

    public S3ClientObjectStore(S3TvfOptions options) {
        bucket = options.getBucket();
        s3Client = createClient(options);
    }

    S3ClientObjectStore(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    private AwsCredentialsProvider createCredentialsProvider(S3TvfOptions options) {
        if (options.hasRoleArn()) {
            return createAssumeRoleCredentialsProvider(options);
        }
        return staticCredentialsProvider(options);
    }

    private StsAssumeRoleCredentialsProvider createAssumeRoleCredentialsProvider(
            S3TvfOptions options) {
        AwsCredentialsProvider sourceCredentialsProvider =
                createStsSourceCredentialsProvider(options);
        stsClient =
                StsClient.builder()
                        .region(Region.of(options.getRegion()))
                        .credentialsProvider(sourceCredentialsProvider)
                        .httpClientBuilder(UrlConnectionHttpClient.builder())
                        .build();
        assumeRoleCredentialsProvider =
                StsAssumeRoleCredentialsProvider.builder()
                        .stsClient(stsClient)
                        .refreshRequest(buildAssumeRoleRequest(options))
                        .build();
        return assumeRoleCredentialsProvider;
    }

    private AwsCredentialsProvider createStsSourceCredentialsProvider(S3TvfOptions options) {
        if (options.hasStaticCredentials()) {
            return staticCredentialsProvider(options);
        }
        defaultCredentialsProvider = DefaultCredentialsProvider.builder().build();
        return defaultCredentialsProvider;
    }

    private static StaticCredentialsProvider staticCredentialsProvider(S3TvfOptions options) {
        return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(options.getAccessKey(), options.getSecretKey()));
    }

    static AssumeRoleRequest buildAssumeRoleRequest(S3TvfOptions options) {
        AssumeRoleRequest.Builder request =
                AssumeRoleRequest.builder()
                        .roleArn(options.getRoleArn())
                        .roleSessionName(ROLE_SESSION_NAME);
        if (options.getExternalId() != null) {
            request.externalId(options.getExternalId());
        }
        return request.build();
    }

    private S3Client createClient(S3TvfOptions options) {
        return S3Client.builder()
                .endpointOverride(URI.create(options.getEndpoint()))
                .region(Region.of(options.getRegion()))
                .credentialsProvider(createCredentialsProvider(options))
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
            s3Client.putObject(
                    request,
                    RequestBody.fromContentProvider(
                            () -> new ByteArrayInputStream(content),
                            content.length,
                            JSON_LINES_CONTENT_TYPE));
        } catch (RuntimeException e) {
            throw new IOException(
                    String.format(
                            "Failed to upload object '%s' to bucket '%s'.", objectKey, bucket),
                    e);
        }
    }

    @Override
    public void close() {
        try {
            s3Client.close();
        } finally {
            closeCredentialsProviders();
        }
    }

    private void closeCredentialsProviders() {
        if (assumeRoleCredentialsProvider != null) {
            assumeRoleCredentialsProvider.close();
        }
        if (stsClient != null) {
            stsClient.close();
        }
        if (defaultCredentialsProvider != null) {
            defaultCredentialsProvider.close();
        }
    }
}
