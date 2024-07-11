package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.migration.auto.BeaconSystem;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashSet;

/**
 * @author lishanglin
 * date 2021/1/19
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultClusterMonitorModifiedNotifierTest extends AbstractConsoleTest {

    @Mock
    private MonitorManager monitorManager;

    @Mock
    private BeaconMetaService beaconMetaService;

    @Mock
    private MonitorService monitorService;

    @Mock
    private ConsoleCommonConfig config;

    @Mock
    private MetaCache metaCache;

    private DefaultClusterMonitorModifiedNotifier notifier;

    @Before
    public void setupDefaultClusterMonitorModifiedNotifierTest() {
        notifier = new DefaultClusterMonitorModifiedNotifier(beaconMetaService, monitorManager, metaCache, config);

        Mockito.when(monitorManager.get(Mockito.anyLong(), Mockito.anyString())).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildCurrentBeaconGroups(Mockito.anyString())).thenReturn(Collections.singleton(new MonitorGroupMeta()));
        Mockito.when(config.getBeaconSupportZone()).thenReturn("SHA");
        Mockito.when(metaCache.getClusterType(Mockito.anyString())).thenReturn(ClusterType.ONE_WAY);
        Mockito.when(metaCache.getActiveDc(Mockito.anyString())).thenReturn("jq");
        Mockito.when(metaCache.isDcInRegion(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
    }

    @Test
    public void testNotifyClusterChange() throws Exception {
        notifier.notifyClusterUpdate("cluster1", 1);
        waitConditionUntilTimeOut(() -> Mockito.mockingDetails(monitorService).getInvocations().size() >= 1);

        Mockito.verify(beaconMetaService).buildCurrentBeaconGroups("cluster1");
        Mockito.verify(monitorService).registerCluster(BeaconSystem.getDefault().getSystemName(), "cluster1", Collections.singleton(new MonitorGroupMeta()));
    }

    @Test
    public void testNotifyClusterDeleted() throws Exception {
        notifier.notifyClusterDelete("cluster1", 1);
        waitConditionUntilTimeOut(() -> Mockito.mockingDetails(monitorService).getInvocations().size() >= 1);

        Mockito.verify(monitorService).unregisterCluster(BeaconSystem.getDefault().getSystemName(), "cluster1");
    }

    @Test
    public void testClusterActiveDcMNotSupport() {
        Mockito.when(metaCache.isDcInRegion(Mockito.anyString(), Mockito.anyString())).thenReturn(false);

        notifier.notifyClusterUpdate("cluster1", 1);
        notifier.notifyClusterDelete("cluster1", 1);
        Assert.assertEquals(0, Mockito.mockingDetails(monitorService).getInvocations().size());
    }

}
