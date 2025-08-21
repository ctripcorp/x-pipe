package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClusterHealthStateTest {

    @Test
    public void testGetLevel() {
        Assert.assertEquals(-1, ClusterHealthState.NORMAL.getLevel());
        Assert.assertEquals(0, ClusterHealthState.LEAST_ONE_DOWN.getLevel());
        Assert.assertEquals(1, ClusterHealthState.QUARTER_DOWN.getLevel());
        Assert.assertEquals(2, ClusterHealthState.HALF_DOWN.getLevel());
        Assert.assertEquals(3, ClusterHealthState.THREE_QUARTER_DOWN.getLevel());
        Assert.assertEquals(4, ClusterHealthState.FULL_DOWN.getLevel());
    }

    @Test
    public void testGetState() {
        for(int i = 20; i > 0; i--) {
            Assert.assertEquals(ClusterHealthState.NORMAL, ClusterHealthState.getState(i, 0));
        }

        Assert.assertEquals(ClusterHealthState.LEAST_ONE_DOWN, ClusterHealthState.getState(10, 1));
        Assert.assertEquals(ClusterHealthState.LEAST_ONE_DOWN, ClusterHealthState.getState(10, 2));
        Assert.assertEquals(ClusterHealthState.QUARTER_DOWN, ClusterHealthState.getState(10, 3));
        Assert.assertEquals(ClusterHealthState.HALF_DOWN, ClusterHealthState.getState(10, 5));
        Assert.assertEquals(ClusterHealthState.THREE_QUARTER_DOWN, ClusterHealthState.getState(10, 8));
        Assert.assertEquals(ClusterHealthState.FULL_DOWN, ClusterHealthState.getState(10, 10));

        Assert.assertEquals(ClusterHealthState.HALF_DOWN, ClusterHealthState.getState(2, 1));
        Assert.assertEquals(ClusterHealthState.FULL_DOWN, ClusterHealthState.getState(2, 2));
        Assert.assertEquals(ClusterHealthState.NORMAL, ClusterHealthState.getState(2, 0));
    }
}