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

package org.apache.doris.flink.table;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisTlsOptions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class DorisRowDataInputFormatTest {

    @Test
    public void testBuilderPropagatesTlsOptions() {
        DorisRowDataInputFormat inputFormat =
                DorisRowDataInputFormat.builder()
                        .setFenodes("fe:8030")
                        .setTableIdentifier("db.table")
                        .setTlsEnabled(true)
                        .setTlsCaCertificatePath("/etc/doris/ca.pem")
                        .setTlsSkipHostnameVerification(true)
                        .setTlsExcludedProtocols("arrowflight")
                        .setPartitions(Collections.emptyList())
                        .setReadOptions(DorisReadOptions.defaults())
                        .setRowType(
                                RowType.of(
                                        new LogicalType[] {new VarCharType()}, new String[] {"c1"}))
                        .build();

        DorisTlsOptions tlsOptions = inputFormat.getOptions().getTlsOptions();
        Assert.assertTrue(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.HTTP));
        Assert.assertTrue(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.MYSQL));
        Assert.assertFalse(tlsOptions.isEnabledFor(DorisTlsOptions.Protocol.ARROW_FLIGHT));
        Assert.assertEquals("/etc/doris/ca.pem", tlsOptions.getCaCertificatePath());
        Assert.assertTrue(tlsOptions.isSkipHostnameVerification());
    }
}
