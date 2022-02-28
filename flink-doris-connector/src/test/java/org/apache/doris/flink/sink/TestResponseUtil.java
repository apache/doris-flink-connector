package org.apache.doris.flink.sink;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestResponseUtil {
//    public static final Pattern LABEL_EXIST_PATTERN =
//            Pattern.compile("errCode = 2, detailMessage = Label \\[(.*)\\] " +
//                    "has already been used, relate to txn \\[(\\d+)\\]");
//    public static final Pattern COMMITTED_PATTERN =
//            Pattern.compile("errCode = 2, detailMessage = transaction \\[(\\d+)\\] " +
//                    "is already COMMITTED, not pre-committed.");
//    public static final Pattern VISIBLE_PATTERN =
//            Pattern.compile("errCode = 2, detailMessage = transaction \\[(\\d+)\\] " +
//                    "is already visible, not pre-committed.");
//
//    public static boolean isCommitted(String msg) {
//        Matcher m = COMMITTED_PATTERN.matcher(msg);
//        if(m.matches()) {
//            return true;
//        }
//        m = VISIBLE_PATTERN.matcher(msg);
//        return m.matches();
//    }
    @Test
    public void testIsCommitted() {
        String committedMsg = "errCode = 2, detailMessage = transaction [2] is already committed, not pre-committed.";
        String visibleMsg = "errCode = 2, detailMessage = transaction [2] is already visible, not pre-committed.";
        String committedMsgWhenAbort = "errCode = 2, detailMessage = transaction [2] is already VISIBLE, not pre-committed.";
        String visibleMsgWhenAbort = "errCode = 2, detailMessage = transaction [2] is already COMMITTED, not pre-committed.";
        String commitMsg = "errCode = 2, detailMessage = transaction [2] is already COMMIT, not pre-committed.";
        String abortedMsg = "errCode = 2, detailMessage = transaction [25] is already aborted. abort reason: User Abort";
        Assert.assertTrue(ResponseUtil.isCommitted(committedMsg));
        Assert.assertTrue(ResponseUtil.isCommitted(visibleMsg));
        Assert.assertTrue(ResponseUtil.isCommitted(committedMsgWhenAbort));
        Assert.assertTrue(ResponseUtil.isCommitted(visibleMsgWhenAbort));
        Assert.assertFalse(ResponseUtil.isCommitted(commitMsg));
        Assert.assertFalse(ResponseUtil.isCommitted(abortedMsg));
    }
}
