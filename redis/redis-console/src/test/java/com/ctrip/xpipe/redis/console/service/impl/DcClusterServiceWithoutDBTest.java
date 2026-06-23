package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.DcClusterCreateInfo;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.resources.ConsolePortalService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DcClusterServiceWithoutDBTest {

    private static final String CLUSTER_NAME = "test-cluster";
    private static final String DC_NAME = "SHARB";
    private static final long CLUSTER_ID = 100L;
    private static final long DC_ID = 1L;

    @Mock
    private ConsolePortalService consolePortalService;

    @Mock
    private ConsoleConfig config;

    @Mock
    private DcService dcService;

    @Mock
    private ClusterService clusterService;

    @InjectMocks
    private DcClusterServiceWithoutDB dcClusterService;

    @Before
    public void setUp() {
        when(config.getCacheRefreshInterval()).thenReturn(Integer.MAX_VALUE);
        when(consolePortalService.findAllClusters()).thenReturn(Collections.singletonList(cluster()));
        when(consolePortalService.getDcClusterInfoOfCluster(eq(CLUSTER_NAME))).thenReturn(portalDcClusters());
        when(dcService.find(DC_NAME)).thenReturn(dc());
        dcClusterService.init();
    }

    @Test
    public void testFindByDcAndClusterName() {
        DcClusterTbl dcCluster = dcClusterService.find(DC_NAME, CLUSTER_NAME);

        Assert.assertNotNull(dcCluster);
        Assert.assertEquals(DC_ID, dcCluster.getDcId());
        Assert.assertEquals(CLUSTER_ID, dcCluster.getClusterId());
        Assert.assertEquals("rule-1", dcCluster.getActiveRedisCheckRules());
    }

    @Test
    public void testFindAllByDcId() {
        List<DcClusterTbl> dcClusters = dcClusterService.findAllByDcId(DC_ID);

        Assert.assertEquals(1, dcClusters.size());
        Assert.assertEquals(CLUSTER_NAME, dcClusters.get(0).getClusterName());
    }

    @Test
    public void testFindClusterRelatedByClusterName() {
        List<DcClusterCreateInfo> related = dcClusterService.findClusterRelated(CLUSTER_NAME);

        Assert.assertEquals(1, related.size());
        Assert.assertEquals(DC_NAME, related.get(0).getDcName());
        Assert.assertEquals(CLUSTER_NAME, related.get(0).getClusterName());
        Assert.assertEquals("rule-1", related.get(0).getRedisCheckRule());
    }

    @Test
    public void testFindClusterRelatedByClusterId() {
        when(clusterService.find(CLUSTER_ID)).thenReturn(cluster());

        List<DcClusterTbl> related = dcClusterService.findClusterRelated(CLUSTER_ID);

        Assert.assertEquals(1, related.size());
        Assert.assertEquals(DC_ID, related.get(0).getDcId());
        Assert.assertEquals(CLUSTER_ID, related.get(0).getClusterId());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWriteOperationUnsupported() {
        dcClusterService.updateDcCluster(new DcClusterCreateInfo()
                .setDcName(DC_NAME).setClusterName(CLUSTER_NAME));
    }

    private ClusterTbl cluster() {
        return new ClusterTbl().setId(CLUSTER_ID).setClusterName(CLUSTER_NAME);
    }

    private DcTbl dc() {
        return new DcTbl().setId(DC_ID).setDcName(DC_NAME);
    }

    private List<DcClusterCreateInfo> portalDcClusters() {
        return Collections.singletonList(new DcClusterCreateInfo()
                .setDcName(DC_NAME)
                .setClusterName(CLUSTER_NAME)
                .setRedisCheckRule("rule-1"));
    }
}
