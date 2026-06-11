package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaComparator;
import com.ctrip.xpipe.redis.core.meta.comparator.InstanceNodeComparator;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RedisMetaSynchronizerTest {

    @Mock
    private RedisService redisService;

    private static final String DC_ID = "jq";
    private static final String CLUSTER_ID = "testCluster";
    private static final String SHARD_ID = "shard1";
    private static final String AZ_NAME = "jq-az1";

    private RedisMeta buildRedisMeta(String ip, int port, String az) {
        ClusterMeta clusterMeta = new ClusterMeta().setId(CLUSTER_ID);
        ShardMeta shardMeta = new ShardMeta().setId(SHARD_ID);
        clusterMeta.addShard(shardMeta);
        RedisMeta redisMeta = new RedisMeta().setIp(ip).setPort(port).setAz(az);
        shardMeta.addRedis(redisMeta);
        return redisMeta;
    }

    @Test
    public void addWithAzCallsInsertWithAzName() throws Exception {
        RedisMeta redisMeta = buildRedisMeta("1.2.3.4", 6379, AZ_NAME);
        Set<com.ctrip.xpipe.redis.core.entity.InstanceNode> added = new HashSet<>();
        added.add(redisMeta);

        RedisMetaSynchronizer synchronizer = new RedisMetaSynchronizer(
                added, Collections.emptySet(), Collections.emptySet(),
                redisService, DC_ID);
        synchronizer.add();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<Pair<String, Integer>, String>> mapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(redisService).insertRedises(eq(DC_ID), eq(CLUSTER_ID), eq(SHARD_ID), mapCaptor.capture());

        Map<Pair<String, Integer>, String> addrToAzName = mapCaptor.getValue();
        assertEquals(1, addrToAzName.size());
        Pair<String, Integer> addr = new Pair<>("1.2.3.4", 6379);
        assertEquals(AZ_NAME, addrToAzName.get(addr));
    }

    @Test
    public void addWithoutAzCallsInsertWithNullAzName() throws Exception {
        RedisMeta redisMeta = buildRedisMeta("1.2.3.4", 6379, null);
        Set<com.ctrip.xpipe.redis.core.entity.InstanceNode> added = new HashSet<>();
        added.add(redisMeta);

        RedisMetaSynchronizer synchronizer = new RedisMetaSynchronizer(
                added, Collections.emptySet(), Collections.emptySet(),
                redisService, DC_ID);
        synchronizer.add();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<Pair<String, Integer>, String>> mapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(redisService).insertRedises(eq(DC_ID), eq(CLUSTER_ID), eq(SHARD_ID), mapCaptor.capture());

        Map<Pair<String, Integer>, String> addrToAzName = mapCaptor.getValue();
        assertEquals(1, addrToAzName.size());
        Pair<String, Integer> addr = new Pair<>("1.2.3.4", 6379);
        assertNull(addrToAzName.get(addr));
    }

    @Test
    public void updateAzChangedCallsUpdateRedisesAz() throws Exception {
        RedisMeta current = buildRedisMeta("1.2.3.4", 6379, "jq-az-old");
        RedisMeta future = buildRedisMeta("1.2.3.4", 6379, AZ_NAME);

        Set<MetaComparator> modified = new HashSet<>();
        modified.add(new InstanceNodeComparator(current, future));

        RedisMetaSynchronizer synchronizer = new RedisMetaSynchronizer(
                Collections.emptySet(), Collections.emptySet(), modified,
                redisService, DC_ID);
        synchronizer.update();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> mapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(redisService).updateRedisesAz(eq(DC_ID), eq(CLUSTER_ID), eq(SHARD_ID), mapCaptor.capture());

        Map<String, String> azMap = mapCaptor.getValue();
        assertEquals(1, azMap.size());
        assertEquals(AZ_NAME, azMap.get("1.2.3.4:6379"));
    }

    @Test
    public void updateAzUnchangedSkipsUpdateRedisesAz() throws Exception {
        RedisMeta current = buildRedisMeta("1.2.3.4", 6379, AZ_NAME);
        RedisMeta future = buildRedisMeta("1.2.3.4", 6379, AZ_NAME);

        Set<MetaComparator> modified = new HashSet<>();
        modified.add(new InstanceNodeComparator(current, future));

        RedisMetaSynchronizer synchronizer = new RedisMetaSynchronizer(
                Collections.emptySet(), Collections.emptySet(), modified,
                redisService, DC_ID);
        synchronizer.update();

        verify(redisService, never()).updateRedisesAz(any(), any(), any(), any());
    }
}
