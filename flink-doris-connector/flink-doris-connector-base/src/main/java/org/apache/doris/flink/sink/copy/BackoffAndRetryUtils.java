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

package org.apache.doris.flink.sink.copy;

import org.apache.doris.flink.exception.DorisRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackoffAndRetryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BackoffAndRetryUtils.class);

    // backoff with 1, 2, 4 seconds
    private static final int[] backoffSec = {0, 1, 2, 4};

    /** Interfaces to define the lambda function to be used by backoffAndRetry. */
    public interface BackoffFunction {
        Object apply() throws Exception;
    }

    public static Object backoffAndRetry(
            final LoadOperation operation, final BackoffFunction runnable) throws Exception {
        String error = "";
        Throwable resExp = null;
        for (int index = 0; index < backoffSec.length; index++) {
            if (index != 0) {
                int second = backoffSec[index];
                Thread.sleep(second * 1000L);
                LOG.info("Retry operation {} {} times", operation, index);
            }
            try {
                return runnable.apply();
            } catch (Exception e) {
                resExp = e;
                error = e.getMessage();
                LOG.error(
                        "Request failed, caught an exception for operation {} with message:{}",
                        operation,
                        e.getMessage());
            }
        }
        String errMsg =
                String.format(
                        "Retry exceeded the max retry limit, operation = %s, error message is %s",
                        operation, error);
        LOG.error(errMsg, resExp);
        throw new DorisRuntimeException(errMsg);
    }

    public enum LoadOperation {
        GET_INTERNAL_STAGE_ADDRESS,
        UPLOAD_FILE,
    }
}
