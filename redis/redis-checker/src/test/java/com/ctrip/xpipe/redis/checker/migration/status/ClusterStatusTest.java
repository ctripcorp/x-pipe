package com.ctrip.xpipe.redis.checker.migration.status;

import org.junit.Assert;
import org.junit.Test;

public class ClusterStatusTest {

    @Test
    public void shouldDetectMigratingStatus() {
        Assert.assertTrue(ClusterStatus.isMigrating(ClusterStatus.Migrating.name()));
        Assert.assertTrue(ClusterStatus.isMigrating("migrating"));
        Assert.assertFalse(ClusterStatus.isMigrating("Normal"));
        Assert.assertFalse(ClusterStatus.isMigrating(null));
    }
}
