package org.apache.doris.flink.source.split;

import org.apache.doris.flink.rest.PartitionDefinition;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class DorisSourceSplitTest {

    @Test
    public void testSplit() {
        PartitionDefinition pd1 =
                new PartitionDefinition("db", "tbl", "be", new HashSet<>(), "queryplan1");
        PartitionDefinition pd2 =
                new PartitionDefinition("db", "tbl", "be", new HashSet<>(), "queryplan1");
        DorisSourceSplit split1 = new DorisSourceSplit(pd1);
        DorisSourceSplit split2 = new DorisSourceSplit(pd2);
        Assert.assertEquals(split1, split2);
    }
}
