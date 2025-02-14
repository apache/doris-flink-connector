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

package org.apache.doris.flink.utils;

import org.apache.doris.flink.util.FastDateUtil;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FastDateUtilTest {

    @Test
    void fastParseDateTimeV2_withValidDateTimeAndPattern_returnsCorrectLocalDateTime() {
        String dateTime = "2023-10-05 14:30:45.123456";
        String pattern = "yyyy-MM-dd HH:mm:ss.SSSSSS";
        LocalDateTime result = FastDateUtil.fastParseDateTimeV2(dateTime, pattern);
        assertEquals(LocalDateTime.of(2023, 10, 5, 14, 30, 45, 123456000), result);
    }

    @Test
    void fastParseDateTime_withValidDateTimeAndPattern_returnsCorrectLocalDateTime() {
        String dateTime = "2023-10-05 14:30:45";
        String pattern = "yyyy-MM-dd HH:mm:ss";
        LocalDateTime result = FastDateUtil.fastParseDateTime(dateTime, pattern);
        assertEquals(LocalDateTime.of(2023, 10, 5, 14, 30, 45), result);
    }

    @Test
    void fastParseDate_withValidDateAndPattern_returnsCorrectLocalDate() {
        String dateTime = "2023-10-05";
        String pattern = "yyyy-MM-dd";
        LocalDate result = FastDateUtil.fastParseDate(dateTime, pattern);
        assertEquals(LocalDate.of(2023, 10, 5), result);
    }
}
