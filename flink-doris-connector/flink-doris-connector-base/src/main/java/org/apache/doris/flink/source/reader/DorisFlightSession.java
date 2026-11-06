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

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlClientWithCallOptions;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlConnection;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Closes the server session without taking ownership of the ADBC connection. */
final class DorisFlightSession implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DorisFlightSession.class);

    private final AdbcConnection connection;
    private final FlightSqlClient client;
    private final CallOption[] callOptions;
    private boolean closed;

    private DorisFlightSession(
            AdbcConnection connection, FlightSqlClient client, CallOption[] callOptions) {
        this.connection = connection;
        this.client = client;
        this.callOptions = Arrays.copyOf(callOptions, callOptions.length + 1);
        this.callOptions[callOptions.length] = CallOptions.timeout(5, TimeUnit.SECONDS);
    }

    static DorisFlightSession open(AdbcDatabase database) throws AdbcException {
        // shortcut: ADBC 0.14.0 hides the client; remove once its close() sends CloseSession.
        Field connectionClient =
                field(FlightSqlConnection.class, "client", FlightSqlClientWithCallOptions.class);
        Field flightClient =
                field(FlightSqlClientWithCallOptions.class, "client", FlightSqlClient.class);
        Field connectionOptions =
                field(
                        FlightSqlClientWithCallOptions.class,
                        "connectionOptions",
                        CallOption[].class);

        // Validate the driver layout before authentication creates a server session.
        AdbcConnection connection = database.connect();
        try {
            Object wrapper = connectionClient.get(connection);
            FlightSqlClient client =
                    Objects.requireNonNull((FlightSqlClient) flightClient.get(wrapper));
            CallOption[] options =
                    Objects.requireNonNull((CallOption[]) connectionOptions.get(wrapper));
            return new DorisFlightSession(connection, client, options);
        } catch (ReflectiveOperationException | RuntimeException failure) {
            try {
                connection.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw new DorisRuntimeException("Cannot bind ADBC Flight SQL session", failure);
        }
    }

    private static Field field(Class<?> owner, String name, Class<?> type) {
        try {
            Field field = owner.getDeclaredField(name);
            if (field.getType() != type) {
                throw new NoSuchFieldException(
                        owner.getSimpleName() + "." + name + " has changed type");
            }
            field.setAccessible(true);
            return field;
        } catch (ReflectiveOperationException | RuntimeException failure) {
            throw new DorisRuntimeException(
                    "Unsupported ADBC Flight SQL driver: cannot access session for CloseSession",
                    failure);
        }
    }

    AdbcConnection getConnection() {
        return connection;
    }

    public synchronized void closeSession() {
        if (closed) {
            return;
        }
        closed = true;
        // Remote session cleanup must not turn a completed read into a failure.
        try {
            CloseSessionResult.Status status =
                    client.closeSession(new CloseSessionRequest(), callOptions).getStatus();
            if (status != CloseSessionResult.Status.CLOSED
                    && status != CloseSessionResult.Status.CLOSING) {
                LOG.warn(
                        "Doris Flight SQL session was not closed, status: {}; it may remain open"
                                + " until the server idle timeout",
                        status);
            }
        } catch (Exception e) {
            LOG.warn(
                    "Failed to close Doris Flight SQL session; it may remain open until the server"
                            + " idle timeout",
                    e);
        }
    }

    @Override
    public void close() {
        closeSession();
    }
}
