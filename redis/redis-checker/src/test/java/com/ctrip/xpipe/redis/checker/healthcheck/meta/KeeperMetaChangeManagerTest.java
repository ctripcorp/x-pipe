package com.ctrip.xpipe.redis.checker.healthcheck.meta;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckInstanceManager;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.KeeperInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.capability.KeeperCapabilityCache;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;

public class KeeperMetaChangeManagerTest {

    @Test
    public void testReconcileIsDcScopedAndNeverRefreshesCapability() throws Exception {
        HealthCheckInstanceManager instances = Mockito.mock(HealthCheckInstanceManager.class);
        KeeperCheckSelector selector = Mockito.mock(KeeperCheckSelector.class);
        KeeperCapabilityCache cache = Mockito.mock(KeeperCapabilityCache.class);
        DcMeta meta = new DcMeta("dc1");
        ClusterMeta cluster = new ClusterMeta().setId("cluster").setType("one_way");
        ShardMeta shard = new ShardMeta().setId("shard");
        shard.addRedis(new RedisMeta().setIp("127.0.0.3").setPort(6379));
        cluster.addShard(shard);
        meta.addCluster(cluster);
        KeeperMeta expected = new KeeperMeta().setIp("127.0.0.2").setPort(6380);
        HostPort removedAddress = new HostPort("127.0.0.1", 6380);
        KeeperHealthCheckInstance removed = instance(removedAddress, "dc1", new RedisSession());
        KeeperHealthCheckInstance retained = instance(new HostPort("127.0.0.2", 6380), "dc1", new RedisSession());
        Mockito.when(selector.select(meta)).thenReturn(Collections.singletonList(expected));
        Mockito.doThrow(new IllegalStateException("cleanup failed")).when(instances).removeKeeper(removedAddress);
        Mockito.when(instances.getKeeperInstancesByDc("dc1"))
                .thenReturn(Collections.singletonList(removed), Collections.singletonList(retained));
        DefaultDcMetaChangeManager manager = new DefaultDcMetaChangeManager("dc1", instances,
                Mockito.mock(HealthCheckEndpointFactory.class), Mockito.mock(MetaCache.class), selector, cache);

        manager.compare(meta);

        Mockito.verify(instances).removeKeeper(removedAddress);
        Mockito.verify(instances).getOrCreate(expected);
        Mockito.verify(instances, Mockito.never()).getAllKeeperInstance();
        Mockito.verify(cache).invalidate(removedAddress);
        Mockito.verify(cache, Mockito.never()).refresh(Mockito.any());

        HostPort retainedAddress = new HostPort(expected.getIp(), expected.getPort());
        Mockito.when(selector.select(Mockito.any(DcMeta.class))).thenReturn(Collections.emptyList());
        Mockito.doThrow(new IllegalStateException("redis cleanup failed"))
                .when(instances).remove(new HostPort("127.0.0.3", 6379));
        try {
            manager.compare(new DcMeta("dc1"));
            Assert.fail("Redis cleanup failure should propagate");
        } catch (IllegalStateException expectedFailure) {
            Mockito.verify(instances).removeKeeper(retainedAddress);
            Mockito.verify(cache).invalidate(retainedAddress);
            Mockito.verify(cache, Mockito.never()).refresh(Mockito.any());
        }

        Mockito.clearInvocations(instances, cache);
        Mockito.when(instances.getKeeperInstancesByDc("dc1")).thenReturn(Collections.singletonList(retained));
        manager.start();
        manager.stop();
        Mockito.verify(instances).removeKeeper(retainedAddress);
        Mockito.verify(cache).invalidateDc("dc1");
        Mockito.verify(cache, Mockito.never()).refresh(Mockito.any());
    }

    @Test
    public void testDefaultDcManagerSourceDoesNotRefreshCapability() throws Exception {
        File file = new File("src/main/java/com/ctrip/xpipe/redis/checker/healthcheck/meta/"
                + "DefaultDcMetaChangeManager.java");
        Assert.assertTrue("source missing: " + file.getAbsolutePath(), file.isFile());
        String source = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        Assert.assertFalse(source.contains("keeperCapabilityCache.refresh("));
    }

    @Test
    public void testTopManagerKeepsDisappearedDcOnMetaAnomaly() {
        DefaultMetaChangeManager manager = new DefaultMetaChangeManager();
        MetaCache metaCache = Mockito.mock(MetaCache.class);
        DcMetaChangeManager dcManager = Mockito.mock(DcMetaChangeManager.class);
        Map<String, DcMetaChangeManager> managers = new java.util.concurrent.ConcurrentHashMap<>();
        managers.put("gone", dcManager);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(new XpipeMeta());
        ReflectionTestUtils.setField(manager, "metaCache", metaCache);
        ReflectionTestUtils.setField(manager, "dcMetaChangeManagers", managers);

        manager.checkDcMetaChange();

        Mockito.verifyNoInteractions(dcManager);
        Assert.assertSame(dcManager, managers.get("gone"));
    }

    private KeeperHealthCheckInstance instance(HostPort address, String dc, RedisSession session) {
        KeeperInstanceInfo info = Mockito.mock(KeeperInstanceInfo.class);
        Mockito.when(info.getHostPort()).thenReturn(address);
        Mockito.when(info.getDcId()).thenReturn(dc);
        KeeperHealthCheckInstance instance = Mockito.mock(KeeperHealthCheckInstance.class);
        Mockito.when(instance.getCheckInfo()).thenReturn(info);
        Mockito.when(instance.getRedisSession()).thenReturn(session);
        return instance;
    }
}
