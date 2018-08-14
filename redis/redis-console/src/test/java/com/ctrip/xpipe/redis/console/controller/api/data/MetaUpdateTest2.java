package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCreateInfo;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.KeeperAdvancedService;
import com.ctrip.xpipe.redis.console.service.KeeperBasicInfo;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Jan 29, 2018
 */
public class MetaUpdateTest2 {

    @Mock
    private RedisService redisService;

    @Mock
    private KeeperAdvancedService keeperAdvancedService;

    @InjectMocks
    private MetaUpdate metaUpdate = new MetaUpdate();

    List<KeeperBasicInfo> keepers;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

    }

    @Test
    public void addKeepers1() throws Exception {
        String cluster = "cluster-test", shard = "shard-test", dc = "SHAJQ";
        when(redisService.findKeepersByDcClusterShard(dc, cluster, shard)).thenReturn(null);

        KeeperBasicInfo keeper1 = new KeeperBasicInfo();
        KeeperBasicInfo keeper2 = new KeeperBasicInfo();
        keeper1.setHost("127.0.0.1");
        keeper1.setPort(6379);
        keeper1.setKeeperContainerId(1);
        keeper2.setHost("127.0.0.1");
        keeper2.setPort(6380);
        keeper2.setKeeperContainerId(1);

        List<KeeperBasicInfo> keepers = Lists.newArrayList(keeper1, keeper2);
        when(keeperAdvancedService.findBestKeepers(eq(dc), eq(RedisProtocol.REDIS_PORT_DEFAULT), any(), eq(cluster)))
                .thenReturn(keepers);

        when(redisService.insertKeepers(dc, cluster, shard, keepers)).thenReturn(2);

        Assert.assertEquals(2, metaUpdate.addKeepers(dc, cluster, new ShardTbl().setShardName(shard)));
    }

    @Test
    public void addKeepers2() throws Exception {
        String cluster = "cluster-test", shard = "shard-test", dc = "SHAJQ";
        when(redisService.findKeepersByDcClusterShard(dc, cluster, shard)).thenReturn(null);

        KeeperBasicInfo keeper1 = new KeeperBasicInfo();
        keeper1.setHost("127.0.0.1");
        keeper1.setPort(6379);
        keeper1.setKeeperContainerId(1);

        keepers = Lists.newArrayList(keeper1);
        when(keeperAdvancedService.findBestKeepers(eq(dc), eq(RedisProtocol.REDIS_PORT_DEFAULT), any(), eq(cluster)))
                .thenReturn(keepers);

        when(redisService.insertKeepers(anyString(), anyString(), anyString(), eq(keepers)))
                .thenReturn(2 - keepers.size());
        Assert.assertEquals(1, metaUpdate.addKeepers(dc, cluster, new ShardTbl().setShardName(shard)));


    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void valvalidateRedisCreateInfo() throws Exception {

        metaUpdate.validateRedisCreateInfo(Lists.newArrayList(
                new RedisCreateInfo().setDcId("jq").setRedises("127.0.0.1:6379"),
                new RedisCreateInfo().setDcId("jq").setRedises("127.0.0.2:6380")));

    }

}