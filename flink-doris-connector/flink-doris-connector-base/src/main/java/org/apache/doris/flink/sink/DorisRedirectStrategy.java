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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.doris.flink.sink;

import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.protocol.HttpContext;

import java.net.URI;

/** Redirect strategy that preserves Doris methods and rejects TLS downgrade redirects. */
public class DorisRedirectStrategy extends DefaultRedirectStrategy {

    private final DorisTlsOptions tlsOptions;

    public DorisRedirectStrategy(DorisTlsOptions tlsOptions) {
        this.tlsOptions = tlsOptions;
    }

    @Override
    protected boolean isRedirectable(String method) {
        return true;
    }

    @Override
    public URI getLocationURI(HttpRequest request, HttpResponse response, HttpContext context)
            throws ProtocolException {
        URI target = super.getLocationURI(request, response, context);
        if (tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.HTTP)
                && request instanceof HttpUriRequest) {
            URI source = ((HttpUriRequest) request).getURI();
            HttpHost sourceHost = HttpClientContext.adapt(context).getTargetHost();
            String sourceScheme =
                    source.getScheme() == null && sourceHost != null
                            ? sourceHost.getSchemeName()
                            : source.getScheme();
            String sourceDescription =
                    source.isAbsolute() || sourceHost == null
                            ? source.toString()
                            : sourceHost.toURI() + source;
            if ("https".equalsIgnoreCase(sourceScheme)
                    && "http".equalsIgnoreCase(target.getScheme())) {
                throw new ProtocolException(
                        "Refusing Doris TLS protocol downgrade redirect from "
                                + sourceDescription
                                + " to "
                                + target);
            }
        }
        return target;
    }
}
