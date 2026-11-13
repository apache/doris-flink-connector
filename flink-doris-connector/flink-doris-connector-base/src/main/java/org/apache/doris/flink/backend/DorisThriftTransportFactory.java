// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.doris.flink.backend;

import org.apache.doris.flink.cfg.ConfigurationOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.doris.flink.connection.DorisTlsContextFactory;
import org.apache.doris.flink.serialization.Routing;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/** Creates Doris BE Thrift transports with per-client TLS configuration. */
final class DorisThriftTransportFactory {

    private DorisThriftTransportFactory() {}

    static TTransport create(
            Routing routing, DorisReadOptions readOptions, DorisTlsOptions tlsOptions)
            throws TTransportException {
        int connectTimeout =
                readOptions.getRequestConnectTimeoutMs() == null
                        ? ConfigurationOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT
                        : readOptions.getRequestConnectTimeoutMs();
        int socketTimeout =
                readOptions.getRequestReadTimeoutMs() == null
                        ? ConfigurationOptions.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT
                        : readOptions.getRequestReadTimeoutMs();
        int maxMessageSize =
                readOptions.getThriftMaxMessageSize() == null
                        ? ConfigurationOptions.DORIS_THRIFT_MAX_MESSAGE_SIZE_DEFAULT
                        : readOptions.getThriftMaxMessageSize();
        TConfiguration configuration =
                TConfiguration.custom().setMaxMessageSize(maxMessageSize).build();

        if (!tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.THRIFT)) {
            return new TSocket(
                    configuration,
                    routing.getHost(),
                    routing.getPort(),
                    socketTimeout,
                    connectTimeout);
        }

        return new TlsTransport(
                configuration,
                routing.getHost(),
                routing.getPort(),
                socketTimeout,
                connectTimeout,
                DorisTlsContextFactory.createSslContext(tlsOptions),
                tlsOptions.isSkipHostnameVerification());
    }

    private static final class TlsTransport extends TIOStreamTransport {
        private final String host;
        private final int port;
        private final int socketTimeout;
        private final int connectTimeout;
        private final SSLContext sslContext;
        private final boolean skipHostnameVerification;

        private SSLSocket socket;

        private TlsTransport(
                TConfiguration configuration,
                String host,
                int port,
                int socketTimeout,
                int connectTimeout,
                SSLContext sslContext,
                boolean skipHostnameVerification)
                throws TTransportException {
            super(configuration);
            this.host = host;
            this.port = port;
            this.socketTimeout = socketTimeout;
            this.connectTimeout = connectTimeout;
            this.sslContext = sslContext;
            this.skipHostnameVerification = skipHostnameVerification;
        }

        @Override
        public boolean isOpen() {
            return socket != null && socket.isConnected() && !socket.isClosed();
        }

        @Override
        public void open() throws TTransportException {
            if (isOpen()) {
                throw new TTransportException(
                        TTransportException.ALREADY_OPEN, "TLS socket is already connected");
            }

            Socket plainSocket = new Socket();
            try {
                plainSocket.connect(new InetSocketAddress(host, port), connectTimeout);
                plainSocket.setSoTimeout(socketTimeout);
                socket =
                        (SSLSocket)
                                sslContext
                                        .getSocketFactory()
                                        .createSocket(plainSocket, host, port, true);
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setEndpointIdentificationAlgorithm(
                        skipHostnameVerification ? null : "HTTPS");
                socket.setSSLParameters(parameters);
                socket.startHandshake();
                inputStream_ = new BufferedInputStream(socket.getInputStream());
                outputStream_ = new BufferedOutputStream(socket.getOutputStream());
            } catch (IOException e) {
                closeSocket(plainSocket);
                close();
                throw new TTransportException(
                        TTransportException.NOT_OPEN,
                        "Unable to open TLS Thrift transport to " + host + ":" + port,
                        e);
            }
        }

        @Override
        public void close() {
            if (socket != null) {
                closeSocket(socket);
            }
            socket = null;
            inputStream_ = null;
            outputStream_ = null;
        }

        private static void closeSocket(Socket socket) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Nothing else can be done while closing the transport.
            }
        }
    }
}
