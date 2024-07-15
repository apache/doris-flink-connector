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

import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.apache.doris.flink.cfg.DorisConnectionOptions;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DorisCatalogTest {

    private DorisCatalog catalog;

    @Before
    public void setup()
            throws DatabaseAlreadyExistException, TableAlreadyExistException,
                    TableNotExistException, DatabaseNotExistException {
        DorisConnectionOptions connectionOptions =
                new DorisConnectionOptions.DorisConnectionOptionsBuilder()
                        .withFenodes("127.0.0.1:8030")
                        .withJdbcUrl("jdbc:mysql://127.0.0.1:8030")
                        .withUsername("root")
                        .withPassword("xxxxx")
                        .build();

        Map<String, String> props = new HashMap<>();
        catalog = new DorisCatalog("catalog_test", connectionOptions, "test", props);
    }

    @Test(expected = Exception.class)
    public void testQueryFenodes() {
        catalog.queryFenodes();
    }
}
