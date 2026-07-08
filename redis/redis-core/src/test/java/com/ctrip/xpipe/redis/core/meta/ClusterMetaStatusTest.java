package com.ctrip.xpipe.redis.core.meta;

import org.junit.Assert;
import org.junit.Test;

public class ClusterMetaStatusTest {

    @Test
    public void shouldDetectMigratingStatus() {
        Assert.assertTrue(ClusterMetaStatus.isMigrating(ClusterMetaStatus.MIGRATING));
        Assert.assertTrue(ClusterMetaStatus.isMigrating("migrating"));
        Assert.assertFalse(ClusterMetaStatus.isMigrating("Normal"));
        Assert.assertFalse(ClusterMetaStatus.isMigrating(null));
    }
}
