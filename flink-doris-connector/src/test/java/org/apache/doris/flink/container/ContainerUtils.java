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

package org.apache.doris.flink.container;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.junit.Assert;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ContainerUtils {

    public static void executeSQLStatement(Connection connection, Logger logger, String... sql) {
        if (Objects.isNull(sql) || sql.length == 0) {
            return;
        }
        try (Statement statement = connection.createStatement()) {
            for (String s : sql) {
                if (StringUtils.isNotEmpty(s)) {
                    logger.info("start to execute sql={}", s);
                    statement.execute(s);
                }
            }
        } catch (SQLException e) {
            throw new DorisRuntimeException(e);
        }
    }

    public static String loadFileContent(String resourcePath) {
        try (InputStream stream =
                ContainerUtils.class.getClassLoader().getResourceAsStream(resourcePath)) {
            return new BufferedReader(new InputStreamReader(Objects.requireNonNull(stream)))
                    .lines()
                    .collect(Collectors.joining("\n"));
        } catch (IOException e) {
            throw new DorisRuntimeException("Failed to read " + resourcePath + " file.", e);
        }
    }

    public static List<String> parseFileArgs(String resourcePath) {
        String fileContent = ContainerUtils.loadFileContent(resourcePath);
        String[] args = fileContent.split("\n");
        List<String> argList = new ArrayList<>();
        for (String arg : args) {
            String[] split = arg.trim().split("\\s+");
            List<String> stringList =
                    Arrays.stream(split)
                            .map(ContainerUtils::removeQuotes)
                            .collect(Collectors.toList());
            argList.addAll(stringList);
        }
        return argList;
    }

    private static String removeQuotes(String str) {
        if (str == null || str.length() < 2) {
            return str;
        }
        if (str.startsWith("\"") && str.endsWith("\"")) {
            return str.substring(1, str.length() - 1);
        }
        if (str.startsWith("\\'") && str.endsWith("\\'")) {
            return str.substring(1, str.length() - 1);
        }
        return str;
    }

    public static String[] parseFileContentSQL(String resourcePath) {
        String fileContent = loadFileContent(resourcePath);
        return Arrays.stream(fileContent.split(";")).map(String::trim).toArray(String[]::new);
    }

    public static void checkResult(
            Connection connection,
            Logger logger,
            List<String> expected,
            String query,
            int columnSize) {
        List<String> actual = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            ResultSet sinkResultSet = statement.executeQuery(query);
            while (sinkResultSet.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columnSize; i++) {
                    Object value = sinkResultSet.getObject(i);
                    if (value == null) {
                        row.add("null");
                    } else {
                        row.add(value.toString());
                    }
                }
                actual.add(StringUtils.join(row, ","));
            }
        } catch (SQLException e) {
            logger.info(
                    "Failed to check query result. expected={}, actual={}",
                    String.join(",", expected),
                    String.join(",", actual),
                    e);
            throw new DorisRuntimeException(e);
        }
        logger.info(
                "checking test result. expected={}, actual={}",
                String.join(",", expected),
                String.join(",", actual));
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }
}
