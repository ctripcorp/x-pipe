package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthState;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultClusterHealthMonitorManagerTest {

    private DefaultClusterHealthMonitorManager manager;

    private ShardService shardService;

    @Before
    public void beforeDefaultClusterHealthMonitorManagerTest() {
        manager = new DefaultClusterHealthMonitorManager();
        shardService = mock(ShardService.class);
        manager.setShardService(shardService);
    }

    @Test
    public void testHealthCheckMasterDown() {
        fakeShardService("cluster", "shard1", "shard2", "shard3", "shard4", "shard5", "shard6", "shard7");
        RedisHealthCheckInstance instance = mock(RedisHealthCheckInstance.class);
        when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo("dc", "cluster", "shard1", null, "", ClusterType.ONE_WAY));
        instance.getRedisInstanceInfo().isMaster(true);
        manager.healthCheckMasterDown(instance);
        Assert.assertEquals(Sets.newHashSet("cluster"), manager.getWarningClusters(ClusterHealthState.LEAST_ONE_DOWN));
        manager.healthCheckMasterDown(instance);
        manager.healthCheckMasterDown(instance);
        manager.healthCheckMasterDown(instance);
        manager.healthCheckMasterDown(instance);
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.HALF_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.THREE_QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.FULL_DOWN));

        when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo("dc", "cluster", "shard2", null, "", ClusterType.ONE_WAY));
        instance.getRedisInstanceInfo().isMaster(true);
        manager.healthCheckMasterDown(instance);

        when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo("dc", "cluster", "shard3", null, "", ClusterType.ONE_WAY));
        instance.getRedisInstanceInfo().isMaster(true);
        manager.healthCheckMasterDown(instance);

        Assert.assertEquals(Sets.newHashSet("cluster"), manager.getWarningClusters(ClusterHealthState.LEAST_ONE_DOWN));
        Assert.assertEquals(Sets.newHashSet("cluster"), manager.getWarningClusters(ClusterHealthState.QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.HALF_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.THREE_QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.FULL_DOWN));
    }

    @Test
    public void testHealthCheckMasterUp() {
        testHealthCheckMasterDown();
        RedisHealthCheckInstance instance = mock(RedisHealthCheckInstance.class);
        when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo("dc", "cluster", "shard8", null, "", ClusterType.ONE_WAY));
        instance.getRedisInstanceInfo().isMaster(true);
        manager.healthCheckMasterUp(instance);

        Assert.assertEquals(Sets.newHashSet("cluster"), manager.getWarningClusters(ClusterHealthState.LEAST_ONE_DOWN));
        Assert.assertEquals(Sets.newHashSet("cluster"), manager.getWarningClusters(ClusterHealthState.QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.HALF_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.THREE_QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.FULL_DOWN));

        when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo("dc", "cluster", "shard3", null, "", ClusterType.ONE_WAY));
        instance.getRedisInstanceInfo().isMaster(true);
        manager.healthCheckMasterUp(instance);

        when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo("dc", "cluster", "shard2", null, "", ClusterType.ONE_WAY));
        instance.getRedisInstanceInfo().isMaster(true);
        manager.healthCheckMasterUp(instance);

        Assert.assertEquals(Sets.newHashSet("cluster"), manager.getWarningClusters(ClusterHealthState.LEAST_ONE_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.HALF_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.THREE_QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.FULL_DOWN));

        when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo("dc", "cluster", "shard1", null, "", ClusterType.ONE_WAY));
        instance.getRedisInstanceInfo().isMaster(true);
        manager.healthCheckMasterUp(instance);

        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.LEAST_ONE_DOWN));
    }

    @Test
    public void testOutterClientMasterDown() {
        fakeShardService("cluster", "shard1", "shard2", "shard3", "shard4", "shard5", "shard6", "shard7");

        manager.outerClientMasterDown("cluster", "shard1");
        Assert.assertEquals(Sets.newHashSet("cluster"), manager.getWarningClusters(ClusterHealthState.LEAST_ONE_DOWN));
        manager.outerClientMasterDown("cluster", "shard1");
        manager.outerClientMasterDown("cluster", "shard1");
        manager.outerClientMasterDown("cluster", "shard1");
        manager.outerClientMasterDown("cluster", "shard1");
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.HALF_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.THREE_QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.FULL_DOWN));

        manager.outerClientMasterDown("cluster", "shard2");

        Assert.assertEquals(Sets.newHashSet("cluster"), manager.getWarningClusters(ClusterHealthState.LEAST_ONE_DOWN));
        Assert.assertEquals(Sets.newHashSet("cluster"), manager.getWarningClusters(ClusterHealthState.QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.HALF_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.THREE_QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.FULL_DOWN));

        fakeShardService("cluster2", "shard1");
        manager.outerClientMasterDown("cluster2", "shard1");
        Assert.assertEquals(Sets.newHashSet("cluster", "cluster2"), manager.getWarningClusters(ClusterHealthState.LEAST_ONE_DOWN));
        Assert.assertEquals(Sets.newHashSet("cluster", "cluster2"), manager.getWarningClusters(ClusterHealthState.QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet("cluster2"), manager.getWarningClusters(ClusterHealthState.HALF_DOWN));
        Assert.assertEquals(Sets.newHashSet("cluster2"), manager.getWarningClusters(ClusterHealthState.THREE_QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet("cluster2"), manager.getWarningClusters(ClusterHealthState.FULL_DOWN));
    }

    @Test
    public void testOutterClientMasterUp() {
        testOutterClientMasterDown();
        manager.outerClientMasterUp("cluster3", "shard1");
        Assert.assertEquals(Sets.newHashSet("cluster", "cluster2"), manager.getWarningClusters(ClusterHealthState.LEAST_ONE_DOWN));
        Assert.assertEquals(Sets.newHashSet("cluster", "cluster2"), manager.getWarningClusters(ClusterHealthState.QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet("cluster2"), manager.getWarningClusters(ClusterHealthState.HALF_DOWN));
        Assert.assertEquals(Sets.newHashSet("cluster2"), manager.getWarningClusters(ClusterHealthState.THREE_QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet("cluster2"), manager.getWarningClusters(ClusterHealthState.FULL_DOWN));

        manager.outerClientMasterUp("cluster2", "shard1");
        Assert.assertEquals(Sets.newHashSet("cluster"), manager.getWarningClusters(ClusterHealthState.LEAST_ONE_DOWN));
        Assert.assertEquals(Sets.newHashSet("cluster"), manager.getWarningClusters(ClusterHealthState.QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.HALF_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.THREE_QUARTER_DOWN));
        Assert.assertEquals(Sets.newHashSet(), manager.getWarningClusters(ClusterHealthState.FULL_DOWN));

    }

    private void fakeShardService(String clusterId, String... shardIds) {
        List<ShardTbl> result = Lists.newArrayList();
        for(String shardId : shardIds) {
            result.add(new ShardTbl().setShardName(shardId));
        }
        when(shardService.findAllShardNamesByClusterName(clusterId)).thenReturn(result);
    }
}