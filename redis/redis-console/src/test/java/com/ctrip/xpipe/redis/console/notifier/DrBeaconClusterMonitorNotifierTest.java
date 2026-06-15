package com.ctrip.xpipe.redis.console.notifier;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class DrBeaconClusterMonitorNotifierTest {

    @Mock
    private BeaconManager beaconManager;

    @Mock
    private MetaCache metaCache;

    @Mock
    private ConsoleCommonConfig config;

    private DrBeaconClusterMonitorNotifier notifier;

    @Before
    public void setUp() {
        notifier = new DrBeaconClusterMonitorNotifier(beaconManager, metaCache, config);
        Mockito.when(config.getBeaconSupportZones()).thenReturn(Collections.emptySet());
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(dualOneWayMeta());
        Mockito.when(metaCache.isDcClusterMigratable(Mockito.anyString(), Mockito.anyString())).thenReturn(true);
    }

    @Test
    public void interestedDcsShouldReturnBothActiveDcsForDualOneWay() {
        Set<String> interestedDcs = notifier.interestedDcs("hetero-dual-oneway");
        Assert.assertEquals(Sets.newHashSet("jq", "fra"), interestedDcs);
    }

    @Test
    public void needNotifyWithoutDcShouldUseInterestedDcs() {
        Assert.assertTrue(notifier.needNotify("hetero-dual-oneway", null, 1));
    }

    @Test
    public void needNotifyWithDcShouldCheckMigratableAndSupportZone() {
        Mockito.when(metaCache.isDcClusterMigratable("hetero-dual-oneway", "oy")).thenReturn(true);
        Mockito.when(config.getBeaconSupportZones()).thenReturn(Collections.singleton("SHA"));
        Mockito.when(metaCache.isDcInRegion("oy", "SHA")).thenReturn(true);

        Assert.assertTrue(notifier.needNotify("hetero-dual-oneway", "oy", 1));
    }

    @Test
    public void needNotifyWithDcShouldNotFallBackToInterestedDcsOnly() {
        Mockito.when(metaCache.isDcClusterMigratable("hetero-dual-oneway", "oy")).thenReturn(false);

        Assert.assertFalse(notifier.needNotify("hetero-dual-oneway", "oy", 1));
    }

    private XpipeMeta dualOneWayMeta() {
        XpipeMeta xpipeMeta = new XpipeMeta();
        DcMeta jq = new DcMeta("jq").setZone("SHA");
        DcMeta oy = new DcMeta("oy").setZone("SHA");
        DcMeta fra = new DcMeta("fra").setZone("FRA");

        ClusterMeta shaCluster = heteroOneWayCluster("jq", "oy");
        jq.addCluster(copyCluster(shaCluster));
        oy.addCluster(copyCluster(shaCluster));

        ClusterMeta fraCluster = heteroOneWayCluster("fra", "");
        fra.addCluster(copyCluster(fraCluster));

        return xpipeMeta.addDc(jq).addDc(oy).addDc(fra);
    }

    private ClusterMeta heteroOneWayCluster(String activeDc, String backupDcs) {
        ClusterMeta clusterMeta = new ClusterMeta("hetero-dual-oneway");
        clusterMeta.setType(ClusterType.HETERO.toString());
        clusterMeta.setAzGroupType(ClusterType.ONE_WAY.toString());
        clusterMeta.setActiveDc(activeDc);
        clusterMeta.setBackupDcs(backupDcs);
        return clusterMeta;
    }

    private ClusterMeta copyCluster(ClusterMeta source) {
        ClusterMeta clusterMeta = new ClusterMeta(source.getId());
        clusterMeta.setType(source.getType());
        clusterMeta.setAzGroupType(source.getAzGroupType());
        clusterMeta.setActiveDc(source.getActiveDc());
        clusterMeta.setBackupDcs(source.getBackupDcs());
        return clusterMeta;
    }
}
