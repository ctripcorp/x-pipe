package com.ctrip.xpipe.redis.console.service.meta.impl;

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

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisMetaServiceImplTest {

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
}
