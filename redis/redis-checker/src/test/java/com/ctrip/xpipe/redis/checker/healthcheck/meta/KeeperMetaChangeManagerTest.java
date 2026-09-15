package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.capability.KeeperCapabilityCache;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import com.ctrip.xpipe.redis.core.meta.comparator.ClusterMetaComparator;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Matchers.any;

public class KeeperMetaChangeManagerTest {

    private HealthCheckInstanceManager instances;
    private KeeperCheckSelector selector;
    private KeeperCapabilityCache cache;
    private HealthCheckEndpointFactory endpointFactory;
    private MetaCache metaCache;
    private DefaultDcMetaChangeManager manager;

    @Before
    public void setUp() {
        instances = Mockito.mock(HealthCheckInstanceManager.class);
        selector = Mockito.mock(KeeperCheckSelector.class);
        cache = Mockito.mock(KeeperCapabilityCache.class);
        endpointFactory = Mockito.mock(HealthCheckEndpointFactory.class);
        metaCache = Mockito.mock(MetaCache.class);
        Mockito.when(selector.select(any(ClusterMeta.class)))
                .thenAnswer(invocation -> keepers(invocation.getArgument(0, ClusterMeta.class)));
        Mockito.when(selector.select(any(DcMeta.class)))
                .thenAnswer(invocation -> keepers(invocation.getArgument(0, DcMeta.class)));
        Mockito.when(selector.shouldLoad(any(KeeperMeta.class))).thenReturn(true);
        manager = new DefaultDcMetaChangeManager("jq", instances, endpointFactory, metaCache, selector, cache);
    }

    @Test
    public void testCollectorConsumesKeeperAddRemoveAndModified() {
        DcMeta current = meta(keeper("10.0.0.1", 6380, 1), keeper("10.0.0.2", 6380, 1));
        DcMeta future = meta(keeper("10.0.0.1", 6380, 2), keeper("10.0.0.3", 6380, 1));
        ClusterMetaComparator comparator = new ClusterMetaComparator(cluster(current), cluster(future));
        comparator.compare();
        KeeperMetaComparatorCollector collector = new KeeperMetaComparatorCollector();

        comparator.accept(collector);
        Pair<List<KeeperMeta>, List<KeeperMeta>> changes = collector.collect();

        Assert.assertEquals(addresses("10.0.0.1:6380", "10.0.0.2:6380"), addresses(changes.getKey()));
        Assert.assertEquals(addresses("10.0.0.1:6380", "10.0.0.3:6380"), addresses(changes.getValue()));
        Assert.assertEquals(Long.valueOf(1), find(changes.getKey(), "10.0.0.1").getKeeperContainerId());
        Assert.assertEquals(Long.valueOf(2), find(changes.getValue(), "10.0.0.1").getKeeperContainerId());
    }

    @Test
    public void testFirstCompareAndNoDiffDoNotTouchKeepers() {
        DcMeta current = meta(keeper("10.0.0.1", 6380, 1));
        manager.compare(current);
        manager.compare(cloneMeta(current));

        Mockito.verify(instances, Mockito.never()).getOrCreate(any(KeeperMeta.class));
        Mockito.verify(instances, Mockito.never()).removeKeeper(any(HostPort.class));
    }

    @Test
    public void testIncrementalKeeperChangesAreRemoveBeforeAddAndInvalidate() {
        DcMeta current = meta(keeper("10.0.0.1", 6380, 1));
        manager.compare(current);
        DcMeta future = cloneMeta(current);
        future.getClusters().get("cluster").getShards().get("shard").getKeepers().get(0).setKeeperContainerId(2L);

        manager.compare(future);

        HostPort address = new HostPort("10.0.0.1", 6380);
        InOrder order = Mockito.inOrder(instances);
        order.verify(instances).removeKeeper(address);
        order.verify(instances).getOrCreate(future.findCluster("cluster").findShard("shard").getKeepers().get(0));
        Mockito.verify(cache).invalidate(address);
    }

    @Test
    public void testShardAndClusterConfigChangesRebuildSelectedKeepers() {
        DcMeta current = meta(keeper("10.0.0.1", 6380, 1));
        manager.compare(current);
        DcMeta shardChanged = cloneMeta(current);
        shardChanged.findCluster("cluster").findShard("shard").setSentinelId(100L);
        manager.compare(shardChanged);
        Mockito.verify(instances).removeKeeper(new HostPort("10.0.0.1", 6380));
        Mockito.verify(instances).getOrCreate(any(KeeperMeta.class));

        Mockito.clearInvocations(instances, cache);
        DcMeta clusterChanged = cloneMeta(shardChanged);
        clusterChanged.findCluster("cluster").setOrgId(2);
        manager.compare(clusterChanged);
        Mockito.verify(instances).removeKeeper(new HostPort("10.0.0.1", 6380));
        Mockito.verify(instances).getOrCreate(any(KeeperMeta.class));
    }

    @Test
    public void testClusterAddRemoveAndDcZoneChangeDriveRedisAndKeeperTogether() {
        DcMeta empty = new DcMeta("jq").setZone("SHA");
        manager.compare(empty);
        DcMeta added = meta(keeper("10.0.0.1", 6380, 1));
        manager.compare(added);
        Mockito.verify(instances).getOrCreate(any(KeeperMeta.class));

        Mockito.clearInvocations(instances, cache);
        DcMeta zoneChanged = cloneMeta(added).setZone("FRA");
        manager.compare(zoneChanged);
        Mockito.verify(instances).removeKeeper(new HostPort("10.0.0.1", 6380));
        Mockito.verify(instances).remove(new HostPort("10.0.0.9", 6379));
        Mockito.verify(instances).getOrCreate(any(RedisMeta.class));
        Mockito.verify(instances).getOrCreate(any(KeeperMeta.class));

        Mockito.clearInvocations(instances, cache);
        manager.compare(new DcMeta("jq").setZone("FRA"));
        Mockito.verify(instances).removeKeeper(new HostPort("10.0.0.1", 6380));
        Mockito.verify(instances, Mockito.never()).getOrCreate(any(KeeperMeta.class));
    }

    @Test
    public void testRemoveFailurePropagatesButStillInvalidates() {
        DcMeta current = meta(keeper("10.0.0.1", 6380, 1));
        manager.compare(current);
        DcMeta future = cloneMeta(current);
        future.findCluster("cluster").findShard("shard").getKeepers().get(0).setKeeperContainerId(2L);
        HostPort address = new HostPort("10.0.0.1", 6380);
        Mockito.doThrow(new IllegalStateException("remove failed")).when(instances).removeKeeper(address);

        try {
            manager.compare(future);
            Assert.fail("Keeper removal failure should follow the Redis propagation contract");
        } catch (IllegalStateException expected) {
            Mockito.verify(cache).invalidate(address);
            Mockito.verify(instances, Mockito.never()).getOrCreate(any(KeeperMeta.class));
        }
    }

    @Test
    public void testManagerStopIsDcScopedAndDisappearedDcIsKept() throws Exception {
        KeeperHealthCheckInstance instance = instance(new HostPort("10.0.0.1", 6380), "jq");
        Mockito.when(instances.getKeeperInstancesByDc("jq")).thenReturn(Collections.singletonList(instance));
        manager.compare(meta(keeper("10.0.0.1", 6380, 1)));
        manager.start();
        manager.stop();
        Mockito.verify(instances).removeKeeper(new HostPort("10.0.0.1", 6380));
        Mockito.verify(cache).invalidateDc("jq");

        DefaultMetaChangeManager top = new DefaultMetaChangeManager();
        MetaCache topMetaCache = Mockito.mock(MetaCache.class);
        DcMetaChangeManager missing = Mockito.mock(DcMetaChangeManager.class);
        Map<String, DcMetaChangeManager> managers = new ConcurrentHashMap<>();
        managers.put("gone", missing);
        Mockito.when(topMetaCache.getXpipeMeta()).thenReturn(new XpipeMeta());
        ReflectionTestUtils.setField(top, "metaCache", topMetaCache);
        ReflectionTestUtils.setField(top, "dcMetaChangeManagers", managers);

        top.checkDcMetaChange();

        Mockito.verifyNoInteractions(missing);
        Assert.assertSame(missing, managers.get("gone"));
    }

    @Test
    public void testSourceHasNoKeeperFullDiffOrCapabilityRefresh() throws Exception {
        File file = new File("src/main/java/com/ctrip/xpipe/redis/checker/healthcheck/meta/DefaultDcMetaChangeManager.java");
        String source = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        Assert.assertFalse(source.contains("reconcileKeepers"));
        Assert.assertFalse(source.contains("keeperCapabilityCache.refresh("));
    }

    private DcMeta meta(KeeperMeta... keepers) {
        DcMeta dc = new DcMeta("jq").setZone("SHA");
        ClusterMeta cluster = new ClusterMeta().setId("cluster").setDbId(1L).setType("one_way").setActiveDc("jq");
        ShardMeta shard = new ShardMeta().setId("shard").setDbId(1L);
        shard.addRedis(new RedisMeta().setIp("10.0.0.9").setPort(6379));
        for (KeeperMeta keeper : keepers) {
            shard.addKeeper(keeper);
        }
        cluster.addShard(shard);
        dc.addCluster(cluster);
        return dc;
    }

    private KeeperMeta keeper(String ip, int port, long containerId) {
        return new KeeperMeta().setIp(ip).setPort(port).setKeeperContainerId(containerId);
    }

    private ClusterMeta cluster(DcMeta dc) {
        return dc.getClusters().get("cluster");
    }

    private List<KeeperMeta> keepers(ClusterMeta cluster) {
        List<KeeperMeta> result = new ArrayList<>();
        cluster.getShards().values().forEach(shard -> result.addAll(shard.getKeepers()));
        return result;
    }

    private List<KeeperMeta> keepers(DcMeta dc) {
        List<KeeperMeta> result = new ArrayList<>();
        dc.getClusters().values().forEach(cluster -> result.addAll(keepers(cluster)));
        return result;
    }

    private DcMeta cloneMeta(DcMeta source) {
        DcMeta clone = MetaCloneFacade.INSTANCE.clone(source);
        clone.getClusters().values().forEach(cluster -> {
            cluster.setParent(clone);
            cluster.getShards().values().forEach(shard -> {
                shard.setParent(cluster);
                shard.getRedises().forEach(redis -> redis.setParent(shard));
                shard.getKeepers().forEach(keeper -> keeper.setParent(shard));
            });
        });
        return clone;
    }

    private KeeperHealthCheckInstance instance(HostPort address, String dc) {
        KeeperInstanceInfo info = Mockito.mock(KeeperInstanceInfo.class);
        Mockito.when(info.getHostPort()).thenReturn(address);
        Mockito.when(info.getDcId()).thenReturn(dc);
        KeeperHealthCheckInstance instance = Mockito.mock(KeeperHealthCheckInstance.class);
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
        return instance;
    }

    private java.util.Set<String> addresses(List<KeeperMeta> keepers) {
        java.util.Set<String> result = new java.util.HashSet<>();
        keepers.forEach(keeper -> result.add(keeper.getIp() + ":" + keeper.getPort()));
        return result;
    }

    private java.util.Set<String> addresses(String... addresses) {
        return new java.util.HashSet<>(java.util.Arrays.asList(addresses));
    }

    private KeeperMeta find(List<KeeperMeta> keepers, String ip) {
        return keepers.stream().filter(keeper -> ip.equals(keeper.getIp())).findFirst().orElse(null);
    }
}
