package org.apache.doris.flink.tools.cdc;

import org.apache.doris.flink.source.split.DorisSourceSplit;
import org.apache.doris.flink.tools.cdc.mysql.MysqlDatabaseSync;
import org.apache.flink.util.StringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 * Unit tests for the {@link DatabaseSync}.
 **/
public class DatabaseSyncTest {
    @Test
    public void multiToOneRulesParserTest() throws Exception{
        String[][] testCase = {
                {"a_.*|b_.*","a|b"} //  Normal condition
//                ,{"a_.*|b_.*","a|b|c"} // Unequal length
//                ,{"",""} // Null value
//                ,{"***....","a"} // Abnormal regular expression
        };
        DatabaseSync databaseSync = new MysqlDatabaseSync();
        Arrays.stream(testCase).forEach(arr->{
            databaseSync.multiToOneRulesParser(arr[0], arr[1]);
        });
    }
}
