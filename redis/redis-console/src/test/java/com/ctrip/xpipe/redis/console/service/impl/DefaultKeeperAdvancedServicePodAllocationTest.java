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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * keeper 分配的双轨行为：容器（pod hostname 为身份）与物理机（IP 为身份）长期并存，
 * 端口记账、去重、落库字段都要按各自的键走。
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
    public void testPodContainerAllocationCarriesHostNameAndAccountsPortsByHostName() {
        when(redisService.findAllRedisWithSameHostName(anyString())).thenReturn(Collections.emptyList());
        mockCandidates(pod(1L, POD_A), pod(2L, POD_B));

        List<KeeperBasicInfo> best = allocate(2);

        Assert.assertEquals(2, best.size());
        Assert.assertEquals(POD_A, best.get(0).getHostName());
        Assert.assertEquals(POD_B, best.get(1).getHostName());
        Assert.assertTrue(best.get(0).isPodKeeper());

        // host 仍是登记时的占位值，真实地址由消费方经 ZK 反查
        Assert.assertEquals(PLACEHOLDER_IP, best.get(0).getHost());

        // 端口记账必须按 hostname 查：所有 pod 的 redis_ip 都是 0.0.0.0，
        // 按 IP 查会把不同 pod 的端口混成一份，导致端口被无谓地跳过
        verify(redisService).findAllRedisWithSameHostName(POD_A);
        verify(redisService).findAllRedisWithSameHostName(POD_B);
        verify(redisService, never()).findAllRedisWithSameIP(anyString());
    }

    @Test
    public void testPodContainerSkipsPortsAlreadyUsedOnSameHostName() {
        // 该 pod 上 6380 与 6381 已被占用
        when(redisService.findAllRedisWithSameHostName(POD_A))
                .thenReturn(Arrays.asList(redisOnPod(POD_A, 6380), redisOnPod(POD_A, 6381)));
        when(redisService.findAllRedisWithSameHostName(POD_B)).thenReturn(Collections.emptyList());
        mockCandidates(pod(1L, POD_A), pod(2L, POD_B));

        List<KeeperBasicInfo> best = allocate(2);

        Assert.assertEquals(6382, best.get(0).getPort());
        // 另一个 pod 的端口空间独立，不受前者影响
        Assert.assertEquals(KEEPER_PORT_DEFAULT, best.get(1).getPort());
    }

    @Test
    public void testPhysicalAndPodMixedAllocationUsesRespectiveKeys() {
        when(redisService.findAllRedisWithSameIP("10.0.0.1"))
                .thenReturn(Collections.singletonList(redisOnIp("10.0.0.1", 6380)));
        when(redisService.findAllRedisWithSameHostName(POD_A)).thenReturn(Collections.emptyList());
        mockCandidates(physical(1L, "10.0.0.1"), pod(2L, POD_A));

        List<KeeperBasicInfo> best = allocate(2);

        // 物理机：按 IP 记账，6380 已用故取 6381，hostName 落空串而非 null
        Assert.assertEquals("10.0.0.1", best.get(0).getHost());
        Assert.assertEquals("", best.get(0).getHostName());
        Assert.assertFalse(best.get(0).isPodKeeper());
        Assert.assertEquals(6381, best.get(0).getPort());

        // 容器：按 hostname 记账，端口空间干净
        Assert.assertEquals(POD_A, best.get(1).getHostName());
        Assert.assertEquals(KEEPER_PORT_DEFAULT, best.get(1).getPort());

        verify(redisService).findAllRedisWithSameIP("10.0.0.1");
        verify(redisService).findAllRedisWithSameHostName(POD_A);
    }

    /**
     * 两个 pod 共享同一个占位 IP 是常态。若去重键仍是 host，第二个 pod 会被误判为
     * "该地址端口已选过"从而被迫跳到下一个端口。
     */
    @Test
    public void testTwoPodsSharingPlaceholderIpGetSamePort() {
        when(redisService.findAllRedisWithSameHostName(anyString())).thenReturn(Collections.emptyList());
        mockCandidates(pod(1L, POD_A), pod(2L, POD_B));

        List<KeeperBasicInfo> best = allocate(2);

        Assert.assertEquals(best.get(0).getHost(), best.get(1).getHost());
        Assert.assertEquals(KEEPER_PORT_DEFAULT, best.get(0).getPort());
        Assert.assertEquals(KEEPER_PORT_DEFAULT, best.get(1).getPort());
    }

    /**
     * 同一台物理机上登记多个 keepercontainer 实例（同 IP、不同管理端口）时，它们共享
     * 一份 keeper 端口空间，记账键必须仍是 IP 而不能退化成 containerId。
     */
    @Test
    public void testSameIpDifferentContainerInstancesShareOnePortSpace() {
        when(redisService.findAllRedisWithSameIP("10.0.0.1")).thenReturn(Collections.emptyList());
        mockCandidates(physical(1L, "10.0.0.1"), physical(2L, "10.0.0.1"));

        List<KeeperBasicInfo> best = allocate(2);

        Assert.assertEquals(KEEPER_PORT_DEFAULT, best.get(0).getPort());
        Assert.assertEquals(KEEPER_PORT_DEFAULT + 1, best.get(1).getPort());
        // 同一个键只查一次
        verify(redisService).findAllRedisWithSameIP("10.0.0.1");
    }

    @Test
    public void testKeeperGoodPredicateReceivesContainerKey() {
        when(redisService.findAllRedisWithSameHostName(anyString())).thenReturn(Collections.emptyList());
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
