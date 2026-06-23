package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.cache.AzGroupCache;
import com.ctrip.xpipe.redis.console.cache.impl.AzGroupCacheImpl;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class AdvancedDcMetaServiceHeteroTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DcMetaService dcMetaService;

    @Autowired
    private AzGroupCache azGroupCache;

    @Before
    public void refreshAzGroupCacheAfterDbReload() throws Exception {
        if (!(azGroupCache instanceof AzGroupCacheImpl)) {
            return;
        }
        Field models = AzGroupCacheImpl.class.getDeclaredField("azGroupModels");
        models.setAccessible(true);
        models.set(azGroupCache, null);
        Field idMap = AzGroupCacheImpl.class.getDeclaredField("idAzGroupMap");
        idMap.setAccessible(true);
        idMap.set(azGroupCache, null);
        azGroupCache.getAllAzGroup();
    }

    @Test
    public void getDcMetaShouldExpandHeteroForOneWayFilter() throws Exception {
        DcMeta dcMeta = dcMetaService.getDcMeta("jq", Collections.singleton(ClusterType.ONE_WAY.toString()));

        ClusterMeta clusterMeta = dcMeta.getClusters().get("hetero-cluster");
        Assert.assertNotNull(clusterMeta);
        Assert.assertEquals(ClusterType.ONE_WAY.toString(), clusterMeta.getType());
        Assert.assertNull(clusterMeta.getAzGroupType());
        Assert.assertEquals("jq", clusterMeta.getActiveDc());
        Assert.assertEquals("oy", clusterMeta.getBackupDcs());

        ClusterMeta dualOneWay = dcMeta.getClusters().get("hetero-dual-oneway");
        Assert.assertNotNull(dualOneWay);
        Assert.assertEquals(ClusterType.ONE_WAY.toString(), dualOneWay.getType());
        Assert.assertEquals("jq", dualOneWay.getActiveDc());
    }

    @Test
    public void getDcMetaShouldKeepDualOneWayRegionsSeparately() throws Exception {
        Set<String> types = new HashSet<>();
        types.add(ClusterType.HETERO.toString());
        types.add(ClusterType.ONE_WAY.toString());

        DcMeta jqMeta = dcMetaService.getDcMeta("jq", types);
        ClusterMeta jqCluster = jqMeta.getClusters().get("hetero-dual-oneway");
        Assert.assertNotNull(jqCluster);
        Assert.assertEquals(ClusterType.ONE_WAY.toString(), jqCluster.getType());
        Assert.assertEquals("jq", jqCluster.getActiveDc());
        Assert.assertEquals("oy", jqCluster.getBackupDcs());

        DcMeta fraMeta = dcMetaService.getDcMeta("fra", types);
        ClusterMeta fraCluster = fraMeta.getClusters().get("hetero-dual-oneway");
        Assert.assertNotNull(fraCluster);
        Assert.assertEquals(ClusterType.ONE_WAY.toString(), fraCluster.getType());
        Assert.assertEquals("fra", fraCluster.getActiveDc());
        Assert.assertEquals("fra-ali", fraCluster.getBackupDcs());
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql");
    }
}
