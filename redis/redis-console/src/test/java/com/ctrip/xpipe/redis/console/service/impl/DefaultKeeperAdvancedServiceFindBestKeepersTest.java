package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.ctrip.xpipe.redis.core.protocal.RedisProtocol.KEEPER_PORT_DEFAULT;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * D36: disk-type filter must run before AZ diversify.
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultKeeperAdvancedServiceFindBestKeepersTest {

    private static final String DC = "jq";
    private static final String CLUSTER = "cluster1";

    @InjectMocks
    private DefaultKeeperAdvancedService keeperAdvancedService;

    @Mock
    private KeeperContainerService keeperContainerService;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private RedisService redisService;

    private ExecutorService executor;

    @Before
    public void setUp() {
        executor = Executors.newSingleThreadExecutor();
        ReflectionTestUtils.setField(keeperAdvancedService, "executor", executor);
        when(redisService.findAllRedisWithSameIP(anyString())).thenReturn(Collections.<RedisTbl>emptyList());
        when(consoleConfig.getKeeperAutoSelectDiskType()).thenReturn("BM");
    }

    @After
    public void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void testFindBestKeepersFiltersDiskTypeBeforeAzDiversify() {
        // Sorted by count ascending: low-count TFS would monopolize AZ slots if AZ ran first.
        KeepercontainerTbl tfsAz1 = kc(1L, "10.0.0.1", "TFS_1", 1L, 1L);
        KeepercontainerTbl tfsAz2 = kc(2L, "10.0.0.2", "TFS_1", 2L, 2L);
        KeepercontainerTbl bmAz1 = kc(3L, "10.0.0.3", "DEFAULT", 10L, 1L);
        KeepercontainerTbl bmAz2 = kc(4L, "10.0.0.4", "DEFAULT", 10L, 2L);

        when(keeperContainerService.findBestKeeperContainersByDcCluster(eq(DC), eq(CLUSTER)))
                .thenReturn(Arrays.asList(tfsAz1, tfsAz2, bmAz1, bmAz2));
        when(keeperContainerService.filterKeeperContainersByAz(anyList(), eq(DC)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        List<KeeperBasicInfo> best = keeperAdvancedService.findBestKeepers(
                DC, KEEPER_PORT_DEFAULT, (ip, port) -> true, CLUSTER, 2);

        ArgumentCaptor<List> azInputCaptor = ArgumentCaptor.forClass(List.class);
        verify(keeperContainerService).findBestKeeperContainersByDcCluster(DC, CLUSTER);
        verify(keeperContainerService).filterKeeperContainersByAz(azInputCaptor.capture(), eq(DC));

        @SuppressWarnings("unchecked")
        List<KeepercontainerTbl> afterDiskType = azInputCaptor.getValue();
        Assert.assertEquals(2, afterDiskType.size());
        for (KeepercontainerTbl kc : afterDiskType) {
            Assert.assertFalse(KeeperDiskTypeUtils.isTfs(kc.getKeepercontainerDiskType()));
        }
        Assert.assertEquals(3L, afterDiskType.get(0).getKeepercontainerId());
        Assert.assertEquals(4L, afterDiskType.get(1).getKeepercontainerId());

        Assert.assertEquals(2, best.size());
        Assert.assertEquals("10.0.0.3", best.get(0).getHost());
        Assert.assertEquals("10.0.0.4", best.get(1).getHost());
    }

    @Test(expected = IllegalStateException.class)
    public void testFindBestKeepersStillFailsWhenTargetDiskTypeInsufficient() {
        KeepercontainerTbl tfsAz1 = kc(1L, "10.0.0.1", "TFS_1", 1L, 1L);
        when(keeperContainerService.findBestKeeperContainersByDcCluster(eq(DC), eq(CLUSTER)))
                .thenReturn(Collections.singletonList(tfsAz1));
        when(keeperContainerService.filterKeeperContainersByAz(anyList(), eq(DC)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        keeperAdvancedService.findBestKeepers(DC, KEEPER_PORT_DEFAULT, (ip, port) -> true, CLUSTER, 2);
    }

    private static KeepercontainerTbl kc(long id, String ip, String diskType, long count, long azId) {
        return new KeepercontainerTbl()
                .setKeepercontainerId(id)
                .setKeepercontainerIp(ip)
                .setKeepercontainerPort(8080)
                .setKeepercontainerDiskType(diskType)
                .setCount(count)
                .setAzId(azId);
    }
}
