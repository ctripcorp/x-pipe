package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.migration.auto.BeaconSystem;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorServiceManager;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
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
    private MonitorServiceManager monitorServiceManager;

    @Mock
    private BeaconMetaService beaconMetaService;

    @Mock
    private MonitorService monitorService;

    @Mock
    private ConsoleConfig config;

    private DefaultClusterMonitorModifiedNotifier notifier;

    @Before
    public void setupDefaultClusterMonitorModifiedNotifierTest() {
        Mockito.when(config.getMigrationUnsupportedClusters()).thenReturn(new HashSet<>());
        notifier = new DefaultClusterMonitorModifiedNotifier(beaconMetaService, monitorServiceManager, config);

        Mockito.when(monitorServiceManager.getOrCreate(Mockito.anyLong())).thenReturn(monitorService);
        Mockito.when(beaconMetaService.buildCurrentBeaconGroups(Mockito.anyString())).thenReturn(Collections.singleton(new MonitorGroupMeta()));
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

}
