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

/** Test for ResponseUtil. */
public class TestResponseUtil {

    @Test
    public void testIsCommitted() {
        String committedMsg =
                "errCode = 2, detailMessage = transaction [2] is already committed, not pre-committed.";
        String visibleMsg =
                "errCode = 2, detailMessage = transaction [2] is already visible, not pre-committed.";
        String committedMsgWhenAbort =
                "errCode = 2, detailMessage = transaction [2] is already VISIBLE, not pre-committed.";
        String visibleMsgWhenAbort =
                "errCode = 2, detailMessage = transaction [2] is already COMMITTED, not pre-committed.";
        String commitMsg =
                "errCode = 2, detailMessage = transaction [2] is already COMMIT, not pre-committed.";
        String abortedMsg =
                "errCode = 2, detailMessage = transaction [25] is already aborted. abort reason: User Abort";
        Assert.assertTrue(ResponseUtil.isCommitted(committedMsg));
        Assert.assertTrue(ResponseUtil.isCommitted(visibleMsg));
        Assert.assertTrue(ResponseUtil.isCommitted(committedMsgWhenAbort));
        Assert.assertTrue(ResponseUtil.isCommitted(visibleMsgWhenAbort));
        Assert.assertFalse(ResponseUtil.isCommitted(commitMsg));
        Assert.assertFalse(ResponseUtil.isCommitted(abortedMsg));
    }
}
