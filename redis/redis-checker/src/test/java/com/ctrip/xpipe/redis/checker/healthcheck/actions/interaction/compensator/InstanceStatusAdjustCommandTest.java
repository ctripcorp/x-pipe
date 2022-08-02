package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

/**
 * @author lishanglin
 * date 2022/8/1
 */
@RunWith(MockitoJUnitRunner.class)
public class InstanceStatusAdjustCommandTest extends AbstractTest {

    @Mock
    private MetaCache metaCache;

    @Mock
    private InstanceHealthStatusCollector collector;

    @Mock
    private OuterClientService outerClientService;

    @Mock
    private CheckerConfig config;

    @Mock
    private AlertManager alertManager;

    @Mock
    private XPipeInstanceHealthHolder xpipeInstanceHealth;

    private int timeoutMilli = 1000;

    private ClusterShardHostPort instance = new ClusterShardHostPort("cluster1", "shard1",
            new HostPort("10.0.0.1", 6379));

    @Before
    public void setupInstanceStatusAdjustCommandTest() throws Exception {
        when(metaCache.inBackupDc(instance.getHostPort())).thenReturn(true);
        when(config.getHealthMarkCompensateIntervalMill()).thenReturn(1000L);
        when(config.getQuorum()).thenReturn(2);
        when(config.isConsoleSiteUnstable()).thenReturn(false);
        when(collector.collectXPipeInstanceHealth(instance.getHostPort())).thenReturn(xpipeInstanceHealth);
    }

    @Test
    public void testAdjust() throws Exception {
        InstanceStatusAdjustCommand cmd = new InstanceStatusAdjustCommand(instance, collector, outerClientService, true,
                System.currentTimeMillis() + timeoutMilli, config, metaCache, alertManager);
        when(outerClientService.isInstanceUp(instance)).thenReturn(false);
        when(xpipeInstanceHealth.aggregate(instance.getHostPort(),2)).thenReturn(Boolean.TRUE);
        cmd.execute().get();
        Mockito.verify(outerClientService).markInstanceUpIfNoModifyFor(instance, 1);
        Mockito.verify(outerClientService, never()).markInstanceDownIfNoModifyFor(instance, 1);
    }

    @Test(expected = ExecutionException.class)
    public void testTimeoutAfterCollect() throws Exception {
        InstanceStatusAdjustCommand cmd = new InstanceStatusAdjustCommand(instance, collector, outerClientService, true,
                System.currentTimeMillis() + timeoutMilli, config, metaCache, alertManager);
        when(outerClientService.isInstanceUp(instance)).thenReturn(false);
        when(xpipeInstanceHealth.aggregate(instance.getHostPort(),2)).thenReturn(Boolean.TRUE);
        doAnswer(inv -> {
            sleep(timeoutMilli + 1);
            return xpipeInstanceHealth;
        }).when(collector).collectXPipeInstanceHealth(instance.getHostPort());

        cmd.execute().get();
    }

    @Test
    public void testSkipForOuterClientChange() throws Exception {
        InstanceStatusAdjustCommand cmd = new InstanceStatusAdjustCommand(instance, collector, outerClientService, true,
                System.currentTimeMillis() + timeoutMilli, config, metaCache, alertManager);
        when(outerClientService.isInstanceUp(instance)).thenReturn(true);
        cmd.execute().get();
        Mockito.verify(outerClientService, never()).markInstanceUpIfNoModifyFor(instance, 1);
        Mockito.verify(outerClientService, never()).markInstanceDownIfNoModifyFor(instance, 1);
    }

    @Test
    public void testSkipForXPipeChange() throws Exception {
        InstanceStatusAdjustCommand cmd = new InstanceStatusAdjustCommand(instance, collector, outerClientService, true,
                System.currentTimeMillis() + timeoutMilli, config, metaCache, alertManager);
        when(outerClientService.isInstanceUp(instance)).thenReturn(false);
        when(xpipeInstanceHealth.aggregate(instance.getHostPort(),2)).thenReturn(null);
        cmd.execute().get();
        Mockito.verify(outerClientService, never()).markInstanceUpIfNoModifyFor(instance, 1);
        Mockito.verify(outerClientService, never()).markInstanceDownIfNoModifyFor(instance, 1);
    }

    @Test
    public void testSiteUnstable() throws Exception {
        InstanceStatusAdjustCommand cmd = new InstanceStatusAdjustCommand(instance, collector, outerClientService, true,
                System.currentTimeMillis() + timeoutMilli, config, metaCache, alertManager);
        when(config.isConsoleSiteUnstable()).thenReturn(true);
        cmd.execute().get();
        Mockito.verify(outerClientService, never()).markInstanceUpIfNoModifyFor(instance, 1);
        Mockito.verify(outerClientService, never()).markInstanceDownIfNoModifyFor(instance, 1);
    }

}
