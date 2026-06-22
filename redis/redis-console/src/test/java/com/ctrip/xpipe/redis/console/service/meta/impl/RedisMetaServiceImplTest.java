package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.cache.AzCache;
import com.ctrip.xpipe.redis.console.model.AzTbl;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Date;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisMetaServiceImplTest extends AbstractTest {

    @InjectMocks
    private RedisMetaServiceImpl redisMetaService;

    @Mock
    private RedisService redisService;

    @Mock
    private AzCache azCache;

    private ShardMeta shardMeta;

    @Before
    public void setUp() {
        shardMeta = new ShardMeta().setId("shard1");
    }

    @Test
    public void getRedisMetaSetsAzNameWhenAzIdPresent() {
        AzTbl azTbl = new AzTbl().setId(10L).setAzName("jq-az1");
        when(azCache.find(10L)).thenReturn(azTbl);

        RedisTbl redisTbl = new RedisTbl()
                .setRedisIp("1.2.3.4")
                .setRedisPort(6379)
                .setAzId(10L);

        RedisMeta result = redisMetaService.getRedisMeta(shardMeta, redisTbl);

        Assert.assertEquals("jq-az1", result.getAz());
        verify(azCache).find(10L);
    }

    @Test
    public void getRedisMetaLeavesAzNullWhenAzIdZero() {
        RedisTbl redisTbl = new RedisTbl()
                .setRedisIp("1.2.3.4")
                .setRedisPort(6379)
                .setAzId(0L);

        RedisMeta result = redisMetaService.getRedisMeta(shardMeta, redisTbl);

        Assert.assertNull(result.getAz());
        verify(azCache, never()).find(anyLong());
    }

    @Test
    public void getRedisMetaLeavesAzNullWhenAzIdNull() {
        RedisTbl redisTbl = new RedisTbl()
                .setRedisIp("1.2.3.4")
                .setRedisPort(6379)
                .setAzId(null);

        RedisMeta result = redisMetaService.getRedisMeta(shardMeta, redisTbl);

        Assert.assertNull(result.getAz());
        verify(azCache, never()).find(anyLong());
    }

    @Test
    public void getRedisMetaLeavesAzNullWhenAzTblNotFound() {
        when(azCache.find(99L)).thenReturn(null);

        RedisTbl redisTbl = new RedisTbl()
                .setRedisIp("1.2.3.4")
                .setRedisPort(6379)
                .setAzId(99L);

        RedisMeta result = redisMetaService.getRedisMeta(shardMeta, redisTbl);

        Assert.assertNull(result.getAz());
    }

    @Test
    public void testGetRedisMetaCopiesCreateTime() {
        RedisMetaServiceImpl redisMetaService = new RedisMetaServiceImpl();
        Date createTime = new Date();
        RedisTbl redisTbl = new RedisTbl();
        redisTbl.setRunId("run-id");
        redisTbl.setRedisIp("127.0.0.1");
        redisTbl.setRedisPort(6379);
        redisTbl.setMaster(true);
        redisTbl.setCreateTime(createTime);

        ShardMeta shardMeta = new ShardMeta();
        RedisMeta redisMeta = redisMetaService.getRedisMeta(shardMeta, redisTbl);

        Assert.assertEquals(createTime.getTime(), redisMeta.getCreateTime().longValue());
    }

    @Test
    public void testGetRedisMetaWithoutCreateTime() {
        RedisMetaServiceImpl redisMetaService = new RedisMetaServiceImpl();
        RedisTbl redisTbl = new RedisTbl();
        redisTbl.setRunId("run-id");
        redisTbl.setRedisIp("127.0.0.1");
        redisTbl.setRedisPort(6379);
        redisTbl.setMaster(true);

        RedisMeta redisMeta = redisMetaService.getRedisMeta(new ShardMeta(), redisTbl);

        Assert.assertNull(redisMeta.getCreateTime());
    }

}