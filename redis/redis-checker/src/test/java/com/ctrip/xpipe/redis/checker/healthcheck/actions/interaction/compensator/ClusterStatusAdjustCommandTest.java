package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.AggregatorPullService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.ClusterActiveDcKey;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.redis.checker.healthcheck.stability.StabilityHolder;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ClusterStatusAdjustCommandTest {

    @Mock
    private MetaCache metaCache;

    @Mock
    private StabilityHolder siteStability;

    @Mock
    private CheckerConfig config;

    @Mock
    private AggregatorPullService aggregatorPullService;

    private int timeoutMilli = 1000;

    private ClusterShardHostPort instance = new ClusterShardHostPort("cluster1", "shard1",
            new HostPort("10.0.0.1", 6379));

    @Test
    public void testSiteUnstable() throws Exception {
        ClusterStatusAdjustCommand cmd = new ClusterStatusAdjustCommand(new ClusterActiveDcKey("cluster1", "jq"), Lists.newArrayList(instance),
                System.currentTimeMillis() + timeoutMilli, siteStability, config, metaCache, aggregatorPullService);
        when(siteStability.isSiteStable()).thenReturn(false);
        cmd.execute().get();
        Mockito.verify(aggregatorPullService, never()).getNeedAdjustInstances(any(), anySet());
        Mockito.verify(aggregatorPullService, never()).doMarkInstancesIfNoModifyFor(anyString(), anyString(), anySet(), anyLong());
    }

    @Test
    public void testBackupDcInstances() throws Exception {
        ClusterStatusAdjustCommand cmd = new ClusterStatusAdjustCommand(new ClusterActiveDcKey("cluster1", "jq"), Lists.newArrayList(instance),
                System.currentTimeMillis() + timeoutMilli, siteStability, config, metaCache, aggregatorPullService);
        when(siteStability.isSiteStable()).thenReturn(true);
        when(metaCache.inBackupDc(instance.getHostPort())).thenReturn(false);
        try {
            cmd.execute().get();
        } catch (Throwable th) {
            Assert.assertTrue(th.getCause() instanceof IllegalArgumentException);
        }
        Mockito.verify(aggregatorPullService, never()).getNeedAdjustInstances(any(), anySet());
        Mockito.verify(aggregatorPullService, never()).doMarkInstancesIfNoModifyFor(anyString(), anyString(), anySet(), anyLong());
    }

    @Test
    public void testNoNeedAdjustInstances() throws Exception {
        ClusterStatusAdjustCommand cmd = new ClusterStatusAdjustCommand(new ClusterActiveDcKey("cluster1", "jq"), Lists.newArrayList(instance),
                System.currentTimeMillis() + timeoutMilli, siteStability, config, metaCache, aggregatorPullService);
        when(siteStability.isSiteStable()).thenReturn(true);
        when(metaCache.inBackupDc(instance.getHostPort())).thenReturn(true);
        when(aggregatorPullService.getNeedAdjustInstances(any(), anySet())).thenReturn(new HashSet<>());

        cmd.execute().get();

        Mockito.verify(aggregatorPullService, times(1)).getNeedAdjustInstances(any(), anySet());
        Mockito.verify(aggregatorPullService, never()).doMarkInstancesIfNoModifyFor(anyString(), anyString(), anySet(), anyLong());
    }

    @Test
    public void testTimeoutAfterCollect() throws Exception {
        ClusterStatusAdjustCommand cmd = new ClusterStatusAdjustCommand(new ClusterActiveDcKey("cluster1", "jq"), Lists.newArrayList(instance),
                System.currentTimeMillis() + timeoutMilli, siteStability, config, metaCache, aggregatorPullService);
        when(siteStability.isSiteStable()).thenReturn(true);
        when(metaCache.inBackupDc(instance.getHostPort())).thenReturn(true);

        doAnswer(inv -> {
            Thread.sleep(timeoutMilli + 1);
            return Sets.newHashSet(new OuterClientService.HostPortDcStatus(instance.getHostPort().getHost(), instance.getHostPort().getPort(), "oy", false));
        }).when(aggregatorPullService).getNeedAdjustInstances(any(), anySet());

        try {
            cmd.execute().get();
        } catch (Throwable th) {
            Assert.assertTrue(th.getCause() instanceof TimeoutException);
        }

        Mockito.verify(aggregatorPullService, times(1)).getNeedAdjustInstances(any(), anySet());
        Mockito.verify(aggregatorPullService, never()).doMarkInstancesIfNoModifyFor(anyString(), anyString(), anySet(), anyLong());
    }


    @Test
    public void testAdjust() throws Exception {
        ClusterStatusAdjustCommand cmd = new ClusterStatusAdjustCommand(new ClusterActiveDcKey("cluster1", "jq"), Lists.newArrayList(instance),
                System.currentTimeMillis() + timeoutMilli, siteStability, config, metaCache, aggregatorPullService);
        when(siteStability.isSiteStable()).thenReturn(true);
        when(metaCache.inBackupDc(instance.getHostPort())).thenReturn(true);
        when(aggregatorPullService.getNeedAdjustInstances(any(), anySet())).thenReturn(Sets.newHashSet(new OuterClientService.HostPortDcStatus(instance.getHostPort().getHost(), instance.getHostPort().getPort(), "oy", false)));

        cmd.execute().get();

        Mockito.verify(aggregatorPullService, times(1)).getNeedAdjustInstances(any(), anySet());
        Mockito.verify(aggregatorPullService, times(1)).doMarkInstancesIfNoModifyFor(anyString(), anyString(), anySet(), anyLong());
    }



}
