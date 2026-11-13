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
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CloseSessionRequest;
import org.apache.arrow.flight.CloseSessionResult;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.doris.flink.exception.DorisRuntimeException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DorisFlightSessionTest {
    @Test
    void closesTheSessionSharedWithAdbcOnlyOnce() throws Exception {
        SessionProducer producer = new SessionProducer();
        try (RootAllocator allocator = new RootAllocator();
                FlightServer server = server(allocator, producer)) {
            Map<String, Object> parameters = parameters(server);
            try (AdbcDatabase database = new FlightSqlDriver(allocator).open(parameters)) {
                DorisFlightSession session = DorisFlightSession.open(database);
                try (AdbcConnection connection = session.getConnection();
                        DorisFlightSession closingSession = session) {
                    assertThat(producer.opens.get()).isEqualTo(1);
                    assertThat(parameters)
                            .containsEntry(AdbcDriver.PARAM_USERNAME.getKey(), "root")
                            .containsEntry(AdbcDriver.PARAM_PASSWORD.getKey(), "password");
                    closingSession.closeSession();
                    closingSession.closeSession();
                    assertThat(producer.closes.get()).isEqualTo(1);
                    assertThat(producer.bearerCalls.get()).isEqualTo(1);
                    assertThat(producer.active.get()).isZero();
                }
            }
            assertThat(producer.closes.get()).isEqualTo(1);
        }
    }

    @Test
    void releasesAdbcResourcesEvenWhenCloseSessionFails() throws Exception {
        SessionProducer producer = new SessionProducer();
        producer.rejectClose = true;
        try (RootAllocator allocator = new RootAllocator();
                FlightServer server = server(allocator, producer);
                AdbcDatabase database = new FlightSqlDriver(allocator).open(parameters(server))) {
            DorisFlightSession session = DorisFlightSession.open(database);
            assertThatCode(() -> DorisFlightValueReader.closeAll(session, session.getConnection()))
                    .doesNotThrowAnyException();
            session.close();
            assertThat(producer.closes.get()).isEqualTo(1);
        }
    }

    @Test
    void boundsUnresponsiveSessionCloseAndContinuesCleanup() throws Exception {
        SessionProducer producer = new SessionProducer();
        producer.hangClose = true;
        try (RootAllocator allocator = new RootAllocator();
                FlightServer server = server(allocator, producer);
                AdbcDatabase database = new FlightSqlDriver(allocator).open(parameters(server))) {
            DorisFlightSession session = DorisFlightSession.open(database);
            long start = System.nanoTime();
            assertThatCode(() -> DorisFlightValueReader.closeAll(session, session.getConnection()))
                    .doesNotThrowAnyException();
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
                    .isBetween(4000L, 10000L);
            session.close();
            assertThat(producer.closes.get()).isEqualTo(1);
        }
    }

    @Test
    void closesConnectionWhenSessionBindingFails() throws Exception {
        AdbcDatabase database = mock(AdbcDatabase.class);
        AdbcConnection connection = mock(AdbcConnection.class);
        when(database.connect()).thenReturn(connection);

        assertThatThrownBy(() -> DorisFlightSession.open(database))
                .isInstanceOf(DorisRuntimeException.class)
                .hasMessageContaining("Cannot bind ADBC Flight SQL session");
        verify(connection).close();
    }

    @Test
    void acceptsSessionCloseInProgress() throws Exception {
        SessionProducer producer = new SessionProducer();
        producer.closeStatus = CloseSessionResult.Status.CLOSING;
        try (RootAllocator allocator = new RootAllocator();
                FlightServer server = server(allocator, producer);
                AdbcDatabase database = new FlightSqlDriver(allocator).open(parameters(server))) {
            DorisFlightSession session = DorisFlightSession.open(database);
            DorisFlightValueReader.closeAll(session, session.getConnection());
            assertThat(producer.closes.get()).isEqualTo(1);
        }
    }

    @Test
    void continuesCleanupWhenTheServerCannotCloseSession() throws Exception {
        SessionProducer producer = new SessionProducer();
        producer.closeStatus = CloseSessionResult.Status.NOT_CLOSABLE;
        try (RootAllocator allocator = new RootAllocator();
                FlightServer server = server(allocator, producer);
                AdbcDatabase database = new FlightSqlDriver(allocator).open(parameters(server))) {
            DorisFlightSession session = DorisFlightSession.open(database);
            assertThatCode(() -> DorisFlightValueReader.closeAll(session, session.getConnection()))
                    .doesNotThrowAnyException();
            assertThat(producer.closes.get()).isEqualTo(1);
            assertThat(producer.active.get()).isEqualTo(1);
        }
    }

    private FlightServer server(RootAllocator allocator, SessionProducer producer)
            throws Exception {
        return FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), producer)
                .headerAuthenticator(producer::authenticate)
                .build()
                .start();
    }

    private Map<String, Object> parameters(FlightServer server) {
        Map<String, Object> parameters = new HashMap<>();
        AdbcDriver.PARAM_URI.set(
                parameters,
                Location.forGrpcInsecure("127.0.0.1", server.getPort()).getUri().toString());
        AdbcDriver.PARAM_USERNAME.set(parameters, "root");
        AdbcDriver.PARAM_PASSWORD.set(parameters, "password");
        return parameters;
    }

    private static class SessionProducer extends NoOpFlightSqlProducer {
        private final AtomicInteger opens = new AtomicInteger();
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final AtomicInteger bearerCalls = new AtomicInteger();
        private boolean rejectClose;
        private boolean hangClose;
        private CloseSessionResult.Status closeStatus = CloseSessionResult.Status.CLOSED;

        private CallHeaderAuthenticator.AuthResult authenticate(CallHeaders headers) {
            String authorization = headers.get("authorization");
            if ("Basic cm9vdDpwYXNzd29yZA==".equals(authorization)) {
                opens.incrementAndGet();
                active.incrementAndGet();
            } else if ("Bearer original-session".equals(authorization) && active.get() == 1) {
                bearerCalls.incrementAndGet();
            } else {
                throw CallStatus.UNAUTHENTICATED.toRuntimeException();
            }
            return new CallHeaderAuthenticator.AuthResult() {
                @Override
                public String getPeerIdentity() {
                    return "original-session";
                }

                @Override
                public void appendToOutgoingHeaders(CallHeaders outgoing) {
                    outgoing.insert("authorization", "Bearer original-session");
                }
            };
        }

        @Override
        public void closeSession(
                CloseSessionRequest request,
                CallContext context,
                StreamListener<CloseSessionResult> listener) {
            closes.incrementAndGet();
            if (hangClose) {
                return;
            }
            if (rejectClose) {
                listener.onError(
                        CallStatus.INTERNAL.withDescription("close refused").toRuntimeException());
                return;
            }
            if (closeStatus == CloseSessionResult.Status.CLOSED) {
                active.decrementAndGet();
            }
            listener.onNext(new CloseSessionResult(closeStatus));
            listener.onCompleted();
        }
    }
}
