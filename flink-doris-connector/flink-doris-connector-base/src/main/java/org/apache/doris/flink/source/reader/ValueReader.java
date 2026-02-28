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

package org.apache.doris.flink.source.reader;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.rest.PartitionDefinition;
import org.apache.doris.flink.rest.RestService;
import org.slf4j.Logger;

import java.util.List;

public abstract class ValueReader {

    public static ValueReader createReader(
            PartitionDefinition partition,
            DorisOptions options,
            DorisReadOptions readOptions,
            Logger logger) {
        logger.info("create reader for partition: {}", partition.toStringWithoutPlan());
        if (readOptions.getUseFlightSql()) {
            Integer adbcPort = RestService.tryGetArrowFlightSqlPort(options, readOptions, logger);
            if (adbcPort != null && adbcPort > 0) {
                readOptions.setFlightSqlPort(adbcPort);
                logger.info(
                        "Using Arrow Flight SQL port to read data, port is: {}.",
                        readOptions.getFlightSqlPort());
                return new DorisFlightValueReader(partition, options, readOptions);
            } else {
                logger.warn(
                        "Arrow Flight SQL port [{}] is invalid or not available. Falling back to Thrift.",
                        adbcPort);
            }
        }
        logger.info("Use thrift to read data");
        return new DorisValueReader(partition, options, readOptions);
    }

    public abstract boolean hasNext();

    public abstract List next();

    public abstract void close() throws Exception;
}
