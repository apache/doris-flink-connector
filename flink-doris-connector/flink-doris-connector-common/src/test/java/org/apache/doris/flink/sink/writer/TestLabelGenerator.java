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

package org.apache.doris.flink.sink.writer;

import org.junit.Assert;
import org.junit.Test;

public class TestLabelGenerator {

    private static String UUID_REGEX_WITHOUT_LINE =
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    @Test
    public void generateTableLabelTest() {
        LabelGenerator labelGenerator = new LabelGenerator("test001", true, "db.table", 0);
        String label = labelGenerator.generateTableLabel(1);
        Assert.assertEquals("test001_db_table_0_1", label);

        // mock label length more than 128
        labelGenerator =
                new LabelGenerator(
                        "test001",
                        false,
                        "db.tabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletable",
                        0);
        label = labelGenerator.generateTableLabel(1);
        Assert.assertTrue(label.matches("test001_0_1_" + UUID_REGEX_WITHOUT_LINE));

        // mock table name chinese
        labelGenerator = new LabelGenerator("test001", false, "数据库.数据表", 0);
        label = labelGenerator.generateTableLabel(1);
        Assert.assertTrue(label.matches("test001_0_1_" + UUID_REGEX_WITHOUT_LINE));

        // mock table name chinese and 2pc
        labelGenerator = new LabelGenerator("test001", true, "数据库.数据表", 0);
        label = labelGenerator.generateTableLabel(1);
        Assert.assertTrue(label.matches("test001_" + UUID_REGEX_WITHOUT_LINE + "_0_1"));
    }

    @Test
    public void generateBatchLabelTest() {
        LabelGenerator labelGenerator = new LabelGenerator("test001", false);
        String label = labelGenerator.generateBatchLabel("table");
        Assert.assertTrue(label.matches("test001_table_" + UUID_REGEX_WITHOUT_LINE));

        // mock label length more than 128
        labelGenerator = new LabelGenerator("test001", false);
        label =
                labelGenerator.generateBatchLabel(
                        "tabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletabletable");
        Assert.assertTrue(label.matches("test001_" + UUID_REGEX_WITHOUT_LINE));

        // mock table name chinese
        labelGenerator = new LabelGenerator("test001", false);
        label = labelGenerator.generateBatchLabel("数据库.数据表");
        Assert.assertTrue(label.matches("test001_" + UUID_REGEX_WITHOUT_LINE));
    }
}
