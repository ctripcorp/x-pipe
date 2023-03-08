package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

/**
 * @author lishanglin
 * date 2022/4/8
 */
@RunWith(MockitoJUnitRunner.class)
public class BeaconBiClusterMonitorRegisterTest extends AbstractConsoleTest {

    private BeaconBiClusterMonitorRegister register;

    @Mock
    private MetaCache metaCache;

    @Mock
    private BeaconManager beaconManager;

    @Mock
    private ConsoleConfig config;

    private String cluster = "cluster1";

    @Before
    public void setupBeaconBiClusterMonitorRegisterTest() {
        register = new BeaconBiClusterMonitorRegister(metaCache, beaconManager, config);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        Mockito.when(config.getClustersSupportBiMigration()).thenReturn(Collections.singleton(cluster));
    }

    @Test
    public void testRegisterBiCluster() throws Exception {
        register.doCheck();
        Mockito.verify(beaconManager).registerCluster(cluster, ClusterType.BI_DIRECTION, 1);
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "crdt-replication-test.xml";
    }

}
