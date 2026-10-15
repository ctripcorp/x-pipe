package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.AzService;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultKeeperAdvancedServiceGetNewKeepersTest {

    @InjectMocks
    private DefaultKeeperAdvancedService keeperAdvancedService;

    @Mock
    private KeeperContainerService keeperContainerService;

    @Mock
    private RedisService redisService;

    @Mock
    private AzService azService;

    private ExecutorService executor;

    private static final String DC_NAME = "dc1";
    private static final String CLUSTER_NAME = "cluster1";
    private static final String SRC_IP = "10.0.0.1";
    private static final String TARGET_IP = "10.0.0.4";
    private static final long SRC_KEEPER_CONTAINER_ID = 1L;
    private static final long TARGET_KEEPER_CONTAINER_ID = 4L;

    @Before
    public void setUp() {
        executor = Executors.newSingleThreadExecutor();
        ReflectionTestUtils.setField(keeperAdvancedService, "executor", executor);
        when(azService.isDcSupportMultiAz(anyString())).thenReturn(false);
    }

    @After
    public void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void testGetNewKeepersWhenTwoRemainAfterRemovingSource() {
        ShardModel shardModel = shardModelWithKeepers(
                keeper(SRC_IP, 6380, SRC_KEEPER_CONTAINER_ID),
                keeper("10.0.0.2", 6381, 2L),
                keeper("10.0.0.3", 6382, 3L));

        // 剩余 keeper 的 az，用于 isDifferentAz
        when(keeperContainerService.find(2L)).thenReturn(physicalContainer(2L, "10.0.0.2"));
        // 目标容器按 id 定位，不再按 IP 反查
        when(keeperContainerService.find(TARGET_KEEPER_CONTAINER_ID)).thenReturn(physicalContainer(TARGET_KEEPER_CONTAINER_ID, TARGET_IP));
        when(redisService.findAllKeepersByKeeperContainerId(anyLong())).thenReturn(Collections.emptyList());

        List<RedisTbl> newKeepers = keeperAdvancedService.getNewKeepers(
                DC_NAME, CLUSTER_NAME, shardModel, SRC_KEEPER_CONTAINER_ID, TARGET_KEEPER_CONTAINER_ID);

        Assert.assertNotNull(newKeepers);
        Assert.assertEquals(3, newKeepers.size());
        // 源容器被剔除，新增的是目标容器
        Assert.assertEquals(TARGET_KEEPER_CONTAINER_ID, newKeepers.get(2).getKeepercontainerId());
        Assert.assertEquals(TARGET_IP, newKeepers.get(2).getRedisIp());
        newKeepers.forEach(keeper -> Assert.assertNotEquals(SRC_KEEPER_CONTAINER_ID, keeper.getKeepercontainerId()));

        verify(keeperContainerService).find(TARGET_KEEPER_CONTAINER_ID);
    }

    @Test
    public void testGetNewKeepersReturnsOnlyRemainingWhenTargetContainerMissing() {
        ShardModel shardModel = shardModelWithKeepers(
                keeper(SRC_IP, 6380, SRC_KEEPER_CONTAINER_ID),
                keeper("10.0.0.2", 6381, 2L));

        when(keeperContainerService.find(2L)).thenReturn(physicalContainer(2L, "10.0.0.2"));
        when(keeperContainerService.find(TARGET_KEEPER_CONTAINER_ID)).thenReturn(null);

        List<RedisTbl> newKeepers = keeperAdvancedService.getNewKeepers(
                DC_NAME, CLUSTER_NAME, shardModel, SRC_KEEPER_CONTAINER_ID, TARGET_KEEPER_CONTAINER_ID);

        Assert.assertEquals(1, newKeepers.size());
        Assert.assertEquals(2L, newKeepers.get(0).getKeepercontainerId());
    }

    private KeepercontainerTbl physicalContainer(long id, String ip) {
        return new KeepercontainerTbl()
                .setKeepercontainerId(id)
                .setKeepercontainerIp(ip)
                .setKeepercontainerHostName("")
                .setKeepercontainerPort(8080)
                .setAzId(1L);
    }

    @Test
    public void testGetNewKeepersInheritsReplacedKeeperPriority() {
        ShardModel shardModel = shardModelWithKeepers(
                keeper(SRC_IP, 6380, SRC_KEEPER_CONTAINER_ID).setKeeperPriority(3),
                keeper("10.0.0.2", 6381, 2L).setKeeperPriority(1));

        when(keeperContainerService.find(2L)).thenReturn(physicalContainer(2L, "10.0.0.2"));
        when(keeperContainerService.find(TARGET_KEEPER_CONTAINER_ID))
                .thenReturn(physicalContainer(TARGET_KEEPER_CONTAINER_ID, TARGET_IP));
        when(redisService.findAllKeepersByKeeperContainerId(anyLong())).thenReturn(Collections.emptyList());

        List<RedisTbl> newKeepers = keeperAdvancedService.getNewKeepers(
                DC_NAME, CLUSTER_NAME, shardModel, SRC_KEEPER_CONTAINER_ID, TARGET_KEEPER_CONTAINER_ID);

        RedisTbl migrated = newKeepers.stream()
                .filter(keeper -> TARGET_KEEPER_CONTAINER_ID == keeper.getKeepercontainerId())
                .findFirst().orElse(null);
        Assert.assertNotNull(migrated);
        Assert.assertEquals(TARGET_IP, migrated.getRedisIp());
        Assert.assertEquals(3, migrated.getKeeperPriority());

        RedisTbl remaining = newKeepers.stream()
                .filter(keeper -> keeper.getKeepercontainerId() == 2L)
                .findFirst().orElse(null);
        Assert.assertNotNull(remaining);
        Assert.assertEquals(1, remaining.getKeeperPriority());
    }

    private ShardModel shardModelWithKeepers(RedisTbl... keepers) {
        ShardModel shardModel = new ShardModel();
        shardModel.setShardTbl(new ShardTbl().setShardName("shard1"));
        List<RedisTbl> keeperList = new ArrayList<>();
        for (RedisTbl keeper : keepers) {
            keeperList.add(keeper);
        }
        shardModel.setKeepers(keeperList);
        return shardModel;
    }

    private RedisTbl keeper(String ip, int port, long keeperContainerId) {
        return new RedisTbl()
                .setRedisIp(ip)
                .setRedisPort(port)
                .setKeepercontainerId(keeperContainerId)
                .setRedisRole(XPipeConsoleConstant.ROLE_KEEPER);
    }
}
