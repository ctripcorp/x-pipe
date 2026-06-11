package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.console.cache.AzCache;
import com.ctrip.xpipe.redis.console.model.AzTbl;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.InstanceNodeComparator;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisMetaSynchronizerTest {

    @Mock
    private RedisService redisService;

    @Mock
    private AzCache azCache;

    private static final String DC_ID = "jq";
    private static final String CLUSTER_ID = "testCluster";
    private static final String SHARD_ID = "shard1";
    private static final String AZ_NAME = "jq-az1";
    private static final long AZ_ID = 42L;

    private RedisMeta buildRedisMeta(String ip, int port, String az) {
        ClusterMeta clusterMeta = new ClusterMeta().setId(CLUSTER_ID);
        ShardMeta shardMeta = new ShardMeta().setId(SHARD_ID);
        clusterMeta.addShard(shardMeta);
        RedisMeta redisMeta = new RedisMeta().setIp(ip).setPort(port).setAz(az);
        shardMeta.addRedis(redisMeta);
        return redisMeta;
    }

    @Before
    public void setUp() {
        AzTbl azTbl = new AzTbl().setId(AZ_ID).setAzName(AZ_NAME);
        when(azCache.find(AZ_NAME)).thenReturn(azTbl);
    }

    @Test
    public void addWithAzCallsInsertWithAzId() throws Exception {
        RedisMeta redisMeta = buildRedisMeta("1.2.3.4", 6379, AZ_NAME);
        Set<com.ctrip.xpipe.redis.core.entity.InstanceNode> added = new HashSet<>();
        added.add(redisMeta);

        RedisMetaSynchronizer synchronizer = new RedisMetaSynchronizer(
                added, Collections.emptySet(), Collections.emptySet(),
                redisService, azCache, DC_ID);
        synchronizer.add();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Pair<String, Integer>>> addrsCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Long> azIdCaptor = ArgumentCaptor.forClass(Long.class);
        verify(redisService).insertRedises(eq(DC_ID), eq(CLUSTER_ID), eq(SHARD_ID), addrsCaptor.capture(), azIdCaptor.capture());

        assertEquals(1, addrsCaptor.getValue().size());
        assertEquals("1.2.3.4", addrsCaptor.getValue().get(0).getKey());
        assertEquals(AZ_ID, azIdCaptor.getValue().longValue());
    }

    @Test
    public void addWithoutAzCallsInsertWithNullAzId() throws Exception {
        RedisMeta redisMeta = buildRedisMeta("1.2.3.4", 6379, null);
        Set<com.ctrip.xpipe.redis.core.entity.InstanceNode> added = new HashSet<>();
        added.add(redisMeta);

        RedisMetaSynchronizer synchronizer = new RedisMetaSynchronizer(
                added, Collections.emptySet(), Collections.emptySet(),
                redisService, azCache, DC_ID);
        synchronizer.add();

        ArgumentCaptor<Long> azIdCaptor = ArgumentCaptor.forClass(Long.class);
        verify(redisService).insertRedises(eq(DC_ID), eq(CLUSTER_ID), eq(SHARD_ID), anyList(), azIdCaptor.capture());
        assertNull(azIdCaptor.getValue());
        verify(azCache, never()).find(anyString());
    }

    @Test
    public void updateAzChangedCallsUpdateRedisesAz() throws Exception {
        RedisMeta current = buildRedisMeta("1.2.3.4", 6379, "jq-az-old");
        RedisMeta future = buildRedisMeta("1.2.3.4", 6379, AZ_NAME);

        Set<MetaComparator> modified = new HashSet<>();
        modified.add(new InstanceNodeComparator(current, future));

        RedisMetaSynchronizer synchronizer = new RedisMetaSynchronizer(
                Collections.emptySet(), Collections.emptySet(), modified,
                redisService, azCache, DC_ID);
        synchronizer.update();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Long>> mapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(redisService).updateRedisesAz(eq(DC_ID), eq(CLUSTER_ID), eq(SHARD_ID), mapCaptor.capture());

        Map<String, Long> azMap = mapCaptor.getValue();
        assertEquals(1, azMap.size());
        assertEquals(AZ_ID, azMap.get("1.2.3.4:6379").longValue());
    }

    @Test
    public void updateAzUnchangedSkipsUpdateRedisesAz() throws Exception {
        RedisMeta current = buildRedisMeta("1.2.3.4", 6379, AZ_NAME);
        RedisMeta future = buildRedisMeta("1.2.3.4", 6379, AZ_NAME);

        Set<MetaComparator> modified = new HashSet<>();
        modified.add(new InstanceNodeComparator(current, future));

        RedisMetaSynchronizer synchronizer = new RedisMetaSynchronizer(
                Collections.emptySet(), Collections.emptySet(), modified,
                redisService, azCache, DC_ID);
        synchronizer.update();

        verify(redisService, never()).updateRedisesAz(any(), any(), any(), any());
    }
}
