package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.AzService;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.KeeperContainerService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultKeeperAdvancedServiceGetNewKeepersTest {

    @Spy
    @InjectMocks
    private DefaultKeeperAdvancedService keeperAdvancedService;

    @Mock
    private KeeperContainerService keeperContainerService;

    @Mock
    private AzService azService;

    private static final String DC_NAME = "dc1";
    private static final String CLUSTER_NAME = "cluster1";
    private static final String SRC_IP = "10.0.0.1";
    private static final String TARGET_IP = "10.0.0.4";

    @Before
    public void setUp() {
        when(azService.isDcSupportMultiAz(anyString())).thenReturn(false);
    }

    @Test
    public void testGetNewKeepersWhenTwoRemainAfterRemovingSource() {
        ShardModel shardModel = shardModelWithKeepers(
                keeper(SRC_IP, 6380, 1L),
                keeper("10.0.0.2", 6381, 2L),
                keeper("10.0.0.3", 6382, 3L));

        when(keeperContainerService.find(2L)).thenReturn(new KeepercontainerTbl().setKeepercontainerId(2L).setAzId(1L));

        KeeperBasicInfo selected = new KeeperBasicInfo();
        selected.setHost(TARGET_IP);
        selected.setPort(6383);
        selected.setKeeperContainerId(4L);
        doReturn(Lists.newArrayList(selected)).when(keeperAdvancedService)
                .findBestKeepersByKeeperContainer(eq(TARGET_IP), anyInt(), any(), eq(1));

        List<RedisTbl> newKeepers = keeperAdvancedService.getNewKeepers(
                DC_NAME, CLUSTER_NAME, shardModel, SRC_IP, TARGET_IP);

        Assert.assertNotNull(newKeepers);
        Assert.assertEquals(3, newKeepers.size());
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
