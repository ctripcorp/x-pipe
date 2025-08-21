package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthMonitor;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthState;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.OsUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultClusterHealthMonitorTest {

    private Logger logger = LoggerFactory.getLogger(DefaultClusterHealthMonitorTest.class);

    private DefaultClusterHealthMonitor monitor;

    private String clusterId = "cluster";

    private MetaCache metaCache;

    @Before
    public void beforeDefaultClusterHealthMonitorTest() {
        metaCache = mock(MetaCache.class);
        monitor = new DefaultClusterHealthMonitor(clusterId, metaCache);
    }

    @Test
    public void testGetClusterId() {
        Assert.assertEquals(clusterId, monitor.getClusterId());
    }

    @Test
    public void testBecomeBetter() {
        Assert.assertEquals(ClusterHealthState.NORMAL, monitor.getState());
        fakeShardService("shard1", "shard2", "shard3", "shard4");
        monitor.healthCheckMasterDown("shard1");
        monitor.healthCheckMasterUp("shard1");
        Assert.assertEquals(ClusterHealthState.NORMAL, monitor.getState());
    }

    @Test
    public void testBecomeWorse() {
        Assert.assertEquals(ClusterHealthState.NORMAL, monitor.getState());
        fakeShardService("shard1", "shard2", "shard3", "shard4");
        monitor.healthCheckMasterDown("shard1");
        monitor.healthCheckMasterDown("shard1");
        monitor.outerClientMasterDown("shard1");
        Assert.assertEquals(ClusterHealthState.QUARTER_DOWN, monitor.getState());

        monitor.outerClientMasterDown("shard2");
        Assert.assertEquals(ClusterHealthState.HALF_DOWN, monitor.getState());

        monitor.healthCheckMasterDown("shard4");
        Assert.assertEquals(ClusterHealthState.THREE_QUARTER_DOWN, monitor.getState());

        monitor.outerClientMasterDown("shard3");
        Assert.assertEquals(ClusterHealthState.FULL_DOWN, monitor.getState());
    }

    @Test
    public void testAddListener() {
        final int[] count = {0};
        monitor.addListener(new ClusterHealthMonitor.Listener() {
            @Override
            public void stateChange(String clusterId, ClusterHealthState pre, ClusterHealthState current) {
                logger.info("[stateChange][{}]{} -> {}", clusterId, pre, current);
                count[0]++;
            }
        });
        fakeShardService("shard1", "shard2", "shard3", "shard4");
        monitor.healthCheckMasterDown("shard1");
        monitor.outerClientMasterDown("shard1");
        monitor.outerClientMasterDown("shard1");

        monitor.healthCheckMasterDown("shard2");

        monitor.healthCheckMasterDown("shard4");

        monitor.outerClientMasterDown("shard3");

        Assert.assertEquals(4, count[0]);
    }

    @Test
    public void testRemoveListener() {
        final int[] count = {0};
        ClusterHealthMonitor.Listener listener = new ClusterHealthMonitor.Listener() {
            @Override
            public void stateChange(String clusterId, ClusterHealthState pre, ClusterHealthState current) {
                logger.info("[stateChange][{}]{} -> {}", clusterId, pre, current);
                count[0]++;
            }
        };
        monitor.addListener(listener);
        fakeShardService("shard1", "shard2", "shard3", "shard4");
        monitor.healthCheckMasterDown("shard1");
        monitor.healthCheckMasterDown("shard1");
        monitor.healthCheckMasterDown("shard1");

        monitor.healthCheckMasterDown("shard2");

        monitor.healthCheckMasterDown("shard4");

        monitor.removeListener(listener);

        monitor.healthCheckMasterDown("shard3");

        Assert.assertEquals(3, count[0]);

    }

    @Test
    @Ignore
    public void testMultiThread() throws InterruptedException {
        ThreadPoolExecutor executors = new ThreadPoolExecutor(OsUtils.getCpuCount(), OsUtils.getCpuCount(), 10, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
        executors.prestartAllCoreThreads();
        fakeShardService("shard1", "shard2", "shard3", "shard4", "shard5", "shard6", "shard7", "shard8", "shard9", "shard10");
        for(int i = 1; i <= 20; i ++) {
            int finalI = i;
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {

                    monitor.healthCheckMasterDown("shard" + (finalI % 11));
                }
            });
        }
        for(int i = 1; i <= 20; i ++) {
            int finalI = i;
            executors.execute(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() throws Exception {

                    monitor.healthCheckMasterUp("shard" + (finalI % 11));
                }
            });
        }
        Thread.sleep(1000);
        Assert.assertEquals(ClusterHealthState.NORMAL, monitor.getState());
    }

    private void fakeShardService(String... shardIds) {
        Set<String> result = new HashSet<>();
        for(String shardId : shardIds) {
            result.add(shardId);
        }
        when(metaCache.getAllShardNamesByClusterName(clusterId)).thenReturn(result);
    }
}