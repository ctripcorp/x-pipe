package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * keeper 分配的双轨行为：容器（pod hostname 为身份）与物理机（IP 为身份）长期并存，
 * 端口记账已统一使用 keepercontainer_id，keeperGood 断言仍使用 IP/hostname 以便调用方区分 pod。
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultKeeperAdvancedServicePodAllocationTest {

    private static final String DC = "jq";
    private static final String CLUSTER = "cluster1";
    private static final String POD_A = "keepercontainer-pod-a-0";
    private static final String POD_B = "keepercontainer-pod-b-0";
    private static final String PLACEHOLDER_IP = "0.0.0.0";

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
        when(consoleConfig.getKeeperAutoSelectDiskType()).thenReturn("ANY");
    }

    @After
    public void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void testPodContainerAllocationCarriesHostNameAndAccountsPortsByKeeperContainerId() {
        when(redisService.findAllKeepersByKeeperContainerId(anyLong())).thenReturn(Collections.emptyList());
        mockCandidates(pod(1L, POD_A), pod(2L, POD_B));

        List<KeeperBasicInfo> best = allocate(2);

        Assert.assertEquals(2, best.size());
        Assert.assertEquals(POD_A, best.get(0).getHostName());
        Assert.assertEquals(POD_B, best.get(1).getHostName());
        Assert.assertTrue(best.get(0).isPodKeeper());

        // host 仍是登记时的占位值，真实地址由消费方经 ZK 反查
        Assert.assertEquals(PLACEHOLDER_IP, best.get(0).getHost());

        // 端口记账已统一使用 keepercontainer_id
        verify(redisService).findAllKeepersByKeeperContainerId(1L);
        verify(redisService).findAllKeepersByKeeperContainerId(2L);
        verify(redisService, never()).findAllRedisWithSameIP(anyString());
        verify(redisService, never()).findAllRedisWithSameHostName(anyString());
    }

    @Test
    public void testPodContainerSkipsPortsAlreadyUsedOnSameKeeperContainer() {
        // 该 pod 上 6380 与 6381 已被占用
        when(redisService.findAllKeepersByKeeperContainerId(1L))
                .thenReturn(Arrays.asList(redisOnPod(POD_A, 6380), redisOnPod(POD_A, 6381)));
        when(redisService.findAllKeepersByKeeperContainerId(2L)).thenReturn(Collections.emptyList());
        mockCandidates(pod(1L, POD_A), pod(2L, POD_B));

        List<KeeperBasicInfo> best = allocate(2);

        Assert.assertEquals(6382, best.get(0).getPort());
        // 另一个 pod 的端口空间独立，不受前者影响
        Assert.assertEquals(KEEPER_PORT_DEFAULT, best.get(1).getPort());
    }

    @Test
    public void testPhysicalAndPodMixedAllocationUsesKeeperContainerId() {
        when(redisService.findAllKeepersByKeeperContainerId(1L))
                .thenReturn(Collections.singletonList(redisOnIp("10.0.0.1", 6380)));
        when(redisService.findAllKeepersByKeeperContainerId(2L)).thenReturn(Collections.emptyList());
        mockCandidates(physical(1L, "10.0.0.1"), pod(2L, POD_A));

        List<KeeperBasicInfo> best = allocate(2);

        // 物理机：6380 已用故取 6381，hostName 落空串而非 null
        Assert.assertEquals("10.0.0.1", best.get(0).getHost());
        Assert.assertEquals("", best.get(0).getHostName());
        Assert.assertFalse(best.get(0).isPodKeeper());
        Assert.assertEquals(6381, best.get(0).getPort());

        // 容器：端口空间独立
        Assert.assertEquals(POD_A, best.get(1).getHostName());
        Assert.assertEquals(KEEPER_PORT_DEFAULT, best.get(1).getPort());

        // 端口记账统一使用 keepercontainer_id
        verify(redisService).findAllKeepersByKeeperContainerId(1L);
        verify(redisService).findAllKeepersByKeeperContainerId(2L);
    }

    /**
     * 两个 pod 共享同一个占位 IP 是常态。由于端口记账使用 keepercontainer_id，
     * 两个 pod 独立分配端口，不会互相干扰。
     */
    @Test
    public void testTwoPodsSharingPlaceholderIpGetSamePort() {
        when(redisService.findAllKeepersByKeeperContainerId(anyLong())).thenReturn(Collections.emptyList());
        mockCandidates(pod(1L, POD_A), pod(2L, POD_B));

        List<KeeperBasicInfo> best = allocate(2);

        Assert.assertEquals(best.get(0).getHost(), best.get(1).getHost());
        Assert.assertEquals(KEEPER_PORT_DEFAULT, best.get(0).getPort());
        Assert.assertEquals(KEEPER_PORT_DEFAULT, best.get(1).getPort());
    }

    /**
     * 不同 keepercontainer 实例（即使同 IP）端口空间独立。
     * 由于端口记账使用 keepercontainer_id，每个容器独立记账。
     */
    @Test
    public void testDifferentContainerInstancesHaveIndependentPortSpace() {
        when(redisService.findAllKeepersByKeeperContainerId(1L)).thenReturn(Collections.emptyList());
        when(redisService.findAllKeepersByKeeperContainerId(2L)).thenReturn(Collections.emptyList());
        mockCandidates(physical(1L, "10.0.0.1"), physical(2L, "10.0.0.1"));

        List<KeeperBasicInfo> best = allocate(2);

        // 两个容器端口空间独立，都分配 6380
        Assert.assertEquals(KEEPER_PORT_DEFAULT, best.get(0).getPort());
        Assert.assertEquals(KEEPER_PORT_DEFAULT, best.get(1).getPort());
        
        // 分别查询每个容器
        verify(redisService).findAllKeepersByKeeperContainerId(1L);
        verify(redisService).findAllKeepersByKeeperContainerId(2L);
    }

    @Test
    public void testKeeperGoodPredicateReceivesContainerKey() {
        when(redisService.findAllKeepersByKeeperContainerId(anyLong())).thenReturn(Collections.emptyList());
        mockCandidates(pod(1L, POD_A), pod(2L, POD_B));

        // 探活断言拿到的第一个参数应是容器标识（此处即 hostname），否则调用方无法区分 pod
        List<KeeperBasicInfo> best = keeperAdvancedService.findBestKeepers(DC, KEEPER_PORT_DEFAULT,
                (containerKey, port) -> !(POD_A.equals(containerKey) && KEEPER_PORT_DEFAULT == port),
                CLUSTER, 2);

        Assert.assertEquals(KEEPER_PORT_DEFAULT + 1, best.get(0).getPort());
        Assert.assertEquals(KEEPER_PORT_DEFAULT, best.get(1).getPort());
    }

    private List<KeeperBasicInfo> allocate(int returnCount) {
        return keeperAdvancedService.findBestKeepers(DC, KEEPER_PORT_DEFAULT,
                (containerKey, port) -> true, CLUSTER, returnCount);
    }

    private void mockCandidates(KeepercontainerTbl... containers) {
        when(keeperContainerService.findBestKeeperContainersByDcCluster(eq(DC), eq(CLUSTER)))
                .thenReturn(Arrays.asList(containers));
        when(keeperContainerService.filterKeeperContainersByAz(anyList(), eq(DC)))
                .thenAnswer(invocation -> invocation.getArgument(0));
    }

    private static KeepercontainerTbl pod(long id, String hostName) {
        return new KeepercontainerTbl()
                .setKeepercontainerId(id)
                .setKeepercontainerIp(PLACEHOLDER_IP)
                .setKeepercontainerHostName(hostName)
                .setKeepercontainerPort(8080)
                .setKeepercontainerDiskType("DEFAULT");
    }

    private static KeepercontainerTbl physical(long id, String ip) {
        return new KeepercontainerTbl()
                .setKeepercontainerId(id)
                .setKeepercontainerIp(ip)
                .setKeepercontainerHostName("")
                .setKeepercontainerPort(8080)
                .setKeepercontainerDiskType("DEFAULT");
    }

    private static RedisTbl redisOnPod(String hostName, int port) {
        return new RedisTbl().setRedisIp(PLACEHOLDER_IP).setHostName(hostName).setRedisPort(port);
    }

    private static RedisTbl redisOnIp(String ip, int port) {
        return new RedisTbl().setRedisIp(ip).setHostName("").setRedisPort(port);
    }
}
