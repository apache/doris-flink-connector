package org.apache.doris.flink.sink;

import org.junit.Assert;
import org.junit.Test;

public class TestDorisCommittableSerializer {
    @Test
    public void testSerialize() throws Exception {
        DorisCommittable expectCommittable = new DorisCommittable("fe:8040", "test", 100);
        DorisCommittableSerializer serializer = new DorisCommittableSerializer();
        DorisCommittable committable = serializer.deserialize(1, serializer.serialize(expectCommittable));
        Assert.assertEquals(expectCommittable, committable);
    }
}
