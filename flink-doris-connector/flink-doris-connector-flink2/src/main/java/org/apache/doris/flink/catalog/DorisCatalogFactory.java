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

package org.apache.doris.flink.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.doris.flink.cfg.DorisConnectionOptions;

import java.util.HashSet;
import java.util.Set;

import static org.apache.doris.flink.catalog.DorisCatalogOptions.DEFAULT_DATABASE;
import static org.apache.doris.flink.catalog.DorisCatalogOptions.TABLE_PROPERTIES_PREFIX;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_BATCH_SIZE;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_DESERIALIZE_ARROW_ASYNC;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_DESERIALIZE_QUEUE_SIZE;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_EXEC_MEM_LIMIT;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_REQUEST_QUERY_TIMEOUT_S;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_REQUEST_READ_TIMEOUT_MS;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_REQUEST_RETRIES;
import static org.apache.doris.flink.table.DorisConfigOptions.DORIS_TABLET_SIZE;
import static org.apache.doris.flink.table.DorisConfigOptions.FENODES;
import static org.apache.doris.flink.table.DorisConfigOptions.IDENTIFIER;
import static org.apache.doris.flink.table.DorisConfigOptions.JDBC_URL;
import static org.apache.doris.flink.table.DorisConfigOptions.PASSWORD;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_BUFFER_COUNT;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_BUFFER_SIZE;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_CHECK_INTERVAL;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_ENABLE_2PC;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_ENABLE_DELETE;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_LABEL_PREFIX;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_MAX_RETRIES;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_PARALLELISM;
import static org.apache.doris.flink.table.DorisConfigOptions.SINK_USE_CACHE;
import static org.apache.doris.flink.table.DorisConfigOptions.SOURCE_USE_OLD_API;
import static org.apache.doris.flink.table.DorisConfigOptions.STREAM_LOAD_PROP_PREFIX;
import static org.apache.doris.flink.table.DorisConfigOptions.TABLE_IDENTIFIER;
import static org.apache.doris.flink.table.DorisConfigOptions.USERNAME;

/** Factory for {@link DorisCatalog}. */
public class DorisCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(JDBC_URL);
        options.add(USERNAME);
        options.add(PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(JDBC_URL);
        options.add(DEFAULT_DATABASE);

        options.add(FENODES);
        options.add(TABLE_IDENTIFIER);
        options.add(USERNAME);
        options.add(PASSWORD);

        options.add(DORIS_TABLET_SIZE);
        options.add(DORIS_REQUEST_CONNECT_TIMEOUT_MS);
        options.add(DORIS_REQUEST_READ_TIMEOUT_MS);
        options.add(DORIS_REQUEST_QUERY_TIMEOUT_S);
        options.add(DORIS_REQUEST_RETRIES);
        options.add(DORIS_DESERIALIZE_ARROW_ASYNC);
        options.add(DORIS_DESERIALIZE_QUEUE_SIZE);
        options.add(DORIS_BATCH_SIZE);
        options.add(DORIS_EXEC_MEM_LIMIT);

        options.add(SINK_CHECK_INTERVAL);
        options.add(SINK_ENABLE_2PC);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_ENABLE_DELETE);
        options.add(SINK_LABEL_PREFIX);
        options.add(SINK_BUFFER_SIZE);
        options.add(SINK_BUFFER_COUNT);
        options.add(SINK_PARALLELISM);
        options.add(SINK_USE_CACHE);

        options.add(SOURCE_USE_OLD_API);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(STREAM_LOAD_PROP_PREFIX, TABLE_PROPERTIES_PREFIX);

        DorisConnectionOptions connectionOptions =
                new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                        .withFenodes(helper.getOptions().get(FENODES))
                        .withJdbcUrl(helper.getOptions().get(JDBC_URL))
                        .withUsername(helper.getOptions().get(USERNAME))
                        .withPassword(helper.getOptions().get(PASSWORD))
                        .build();
        return new DorisCatalog(
                context.getName(),
                connectionOptions,
                helper.getOptions().get(DEFAULT_DATABASE),
                ((Configuration) helper.getOptions()).toMap());
    }
}
