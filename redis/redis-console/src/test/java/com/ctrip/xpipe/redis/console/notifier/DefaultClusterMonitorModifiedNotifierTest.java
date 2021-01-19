package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.beacon.BeaconService;
import com.ctrip.xpipe.redis.console.beacon.BeaconServiceManager;
import com.ctrip.xpipe.redis.console.beacon.data.BeaconGroupMeta;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;

/**
 * @author lishanglin
 * date 2021/1/19
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultClusterMonitorModifiedNotifierTest extends AbstractConsoleTest {

    @Mock
    private BeaconServiceManager beaconServiceManager;

    @Mock
    private BeaconMetaService beaconMetaService;

    @Mock
    private BeaconService beaconService;

    private DefaultClusterMonitorModifiedNotifier notifier;

    @Before
    public void setupDefaultClusterMonitorModifiedNotifierTest() {
        notifier = new DefaultClusterMonitorModifiedNotifier(beaconMetaService, beaconServiceManager);

        Mockito.when(beaconServiceManager.getOrCreate(Mockito.anyLong())).thenReturn(beaconService);
        Mockito.when(beaconMetaService.buildCurrentBeaconGroups(Mockito.anyString())).thenReturn(Collections.singleton(new BeaconGroupMeta()));
    }

    @Test
    public void testNotifyClusterChange() throws Exception {
        notifier.notifyClusterUpdate("cluster1", 1);
        waitConditionUntilTimeOut(() -> Mockito.mockingDetails(beaconService).getInvocations().size() >= 1);

        Mockito.verify(beaconMetaService).buildCurrentBeaconGroups("cluster1");
        Mockito.verify(beaconService).registerCluster("cluster1", Collections.singleton(new BeaconGroupMeta()));
    }

    @Test
    public void testNotifyClusterDeleted() throws Exception {
        notifier.notifyClusterDelete("cluster1", 1);
        waitConditionUntilTimeOut(() -> Mockito.mockingDetails(beaconService).getInvocations().size() >= 1);

        Mockito.verify(beaconService).unregisterCluster("cluster1");
    }

}
