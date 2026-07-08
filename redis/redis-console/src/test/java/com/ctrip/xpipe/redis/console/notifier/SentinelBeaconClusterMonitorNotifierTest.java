package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class SentinelBeaconClusterMonitorNotifierTest {

    private static final String CLUSTER = "cluster1";

    private static final String DC = "jq";

    @Mock
    private BeaconManager beaconManager;

    @Mock
    private MetaCache metaCache;

    @Mock
    private ConsoleConfig consoleConfig;

    private SentinelBeaconClusterMonitorNotifier notifier;

    @Before
    public void setup() {
        notifier = new SentinelBeaconClusterMonitorNotifier(beaconManager, metaCache, consoleConfig);
        Mockito.when(consoleConfig.supportSentinelBeacon(1L, CLUSTER)).thenReturn(true);
        Mockito.when(metaCache.getClusterType(CLUSTER)).thenReturn(ClusterType.SINGLE_DC);

        ClusterMeta clusterMeta = new ClusterMeta(CLUSTER);
        clusterMeta.setType(ClusterType.SINGLE_DC.name());
        clusterMeta.setActiveDc(DC);
        DcMeta dcMeta = new DcMeta(DC);
        dcMeta.addCluster(clusterMeta);
        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(dcMeta);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
    }

    @Test
    public void needNotifyShouldReturnFalseWhenSentinelBeaconDisabled() {
        Mockito.when(consoleConfig.supportSentinelBeacon(1L, CLUSTER)).thenReturn(false);
        Assert.assertFalse(notifier.needNotify(CLUSTER, DC, 1L));
    }

    @Test
    public void notifyClusterUpdateShouldRegisterSentinelBeacon() {
        notifier.notifyClusterUpdate(CLUSTER, DC, 1L, "20200101103030001");

        Mockito.verify(beaconManager).registerCluster(CLUSTER, DC, ClusterType.SINGLE_DC, 1, "20200101103030001",
                BeaconRouteType.SENTINEL);
    }

    @Test
    public void notifyClusterDeleteShouldUnregisterSentinelBeacon() {
        notifier.notifyClusterDelete(CLUSTER, DC, 1L);

        Mockito.verify(beaconManager).unregisterCluster(CLUSTER, DC, ClusterType.SINGLE_DC, 1,
                BeaconRouteType.SENTINEL);
    }
}
