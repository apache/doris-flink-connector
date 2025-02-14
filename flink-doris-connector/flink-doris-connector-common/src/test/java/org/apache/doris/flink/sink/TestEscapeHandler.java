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

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static org.apache.doris.flink.sink.writer.LoadConstants.FIELD_DELIMITER_KEY;
import static org.apache.doris.flink.sink.writer.LoadConstants.LINE_DELIMITER_KEY;

/** test for EscapeHandler. */
public class TestEscapeHandler {
    @Test
    public void testHandle() {
        Properties properties = new Properties();
        properties.setProperty(FIELD_DELIMITER_KEY, "\\x09\\x09");
        properties.setProperty(LINE_DELIMITER_KEY, "\\x0A\\x0A");
        EscapeHandler.handleEscape(properties);
        Assert.assertEquals("\t\t", properties.getProperty(FIELD_DELIMITER_KEY));
        Assert.assertEquals("\n\n", properties.getProperty(LINE_DELIMITER_KEY));
    }
}
