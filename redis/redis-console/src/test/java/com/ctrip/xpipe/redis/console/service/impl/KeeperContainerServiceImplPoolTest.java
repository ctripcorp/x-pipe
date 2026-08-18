package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KeeperContainerServiceImplPoolTest {

    @InjectMocks
    private KeeperContainerServiceImpl keeperContainerService;

    @Mock
    private ConsoleConfig consoleConfig;

    @Test
    public void testResolveKeeperContainerPoolDegradeToOrg() {
        when(consoleConfig.isKeeperPoolDegradeToOrg()).thenReturn(true);
        ClusterTbl cluster = new ClusterTbl().setClusterOrgId(100L).setLogicalBuId(5L);
        List<KeepercontainerTbl> all = Arrays.asList(
                keeperContainer(1, 100L, 5L),
                keeperContainer(2, 200L, 5L));

        List<KeepercontainerTbl> pool = keeperContainerService.resolveKeeperContainerPool(cluster, all);

        Assert.assertEquals(1, pool.size());
        Assert.assertEquals(100L, pool.get(0).getKeepercontainerOrgId());
    }

    @Test
    public void testResolveKeeperContainerPoolByLogicalBu() {
        when(consoleConfig.isKeeperPoolDegradeToOrg()).thenReturn(false);
        ClusterTbl cluster = new ClusterTbl().setClusterOrgId(100L).setLogicalBuId(5L);
        List<KeepercontainerTbl> all = Arrays.asList(
                keeperContainer(1, 100L, 5L),
                keeperContainer(2, 100L, 6L),
                keeperContainer(3, 200L, 5L));

        List<KeepercontainerTbl> pool = keeperContainerService.resolveKeeperContainerPool(cluster, all);

        Assert.assertEquals(2, pool.size());
        Assert.assertTrue(pool.stream().allMatch(kc -> kc.getLogicalBuId() == 5L));
    }

    @Test
    public void testResolveKeeperContainerPoolFallbackToOrgWhenBuPoolEmpty() {
        when(consoleConfig.isKeeperPoolDegradeToOrg()).thenReturn(false);
        ClusterTbl cluster = new ClusterTbl().setClusterOrgId(100L).setLogicalBuId(5L);
        List<KeepercontainerTbl> all = Arrays.asList(keeperContainer(1, 100L, 6L));

        List<KeepercontainerTbl> pool = keeperContainerService.resolveKeeperContainerPool(cluster, all);

        Assert.assertEquals(1, pool.size());
        Assert.assertEquals(100L, pool.get(0).getKeepercontainerOrgId());
    }

    @Test
    public void testResolveKeeperContainerPoolByOrgWhenLogicalBuNotBound() {
        when(consoleConfig.isKeeperPoolDegradeToOrg()).thenReturn(false);
        ClusterTbl cluster = new ClusterTbl().setClusterOrgId(100L).setLogicalBuId(0L);
        List<KeepercontainerTbl> all = Arrays.asList(
                keeperContainer(1, 100L, 5L),
                keeperContainer(2, 200L, 6L));

        List<KeepercontainerTbl> pool = keeperContainerService.resolveKeeperContainerPool(cluster, all);

        Assert.assertEquals(1, pool.size());
        Assert.assertEquals(100L, pool.get(0).getKeepercontainerOrgId());
    }

    private KeepercontainerTbl keeperContainer(long id, long orgId, long logicalBuId) {
        return new KeepercontainerTbl()
                .setKeepercontainerId(id)
                .setKeepercontainerOrgId(orgId)
                .setLogicalBuId(logicalBuId);
    }
}
