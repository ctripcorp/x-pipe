package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.GcOrphanStoreResult;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerServiceFactory;
import com.ctrip.xpipe.redis.meta.server.cluster.CurrentClusterServer;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class OrphanStoreGcTaskTest {

    @Mock
    private MetaServerConfig config;
    @Mock
    private DcMetaCache metaCache;
    @Mock
    private CurrentClusterServer currentClusterServer;
    @Mock
    private KeeperContainerServiceFactory keeperContainerServiceFactory;

    private OrphanStoreGcTask task;
    private ExecutorService groupExecutor;

    private KeeperContainerMeta tfsContainerA; // fsId "fs1"
    private KeeperContainerMeta tfsContainerB; // fsId "fs1", same group as A
    private KeeperContainerMeta bmContainer;   // BM, must never enter a group

    @Before
    public void setUp() {
        task = new OrphanStoreGcTask();
        task.setConfig(config);
        task.setMetaCache(metaCache);
        task.setCurrentClusterServer(currentClusterServer);
        task.setKeeperContainerServiceFactory(keeperContainerServiceFactory);
        task.setScheduled(mock(ScheduledExecutorService.class));
        // Single-threaded: tests assert outcomes and call sequences, not timing.
        groupExecutor = Executors.newSingleThreadExecutor();
        task.setGroupExecutor(groupExecutor);

        tfsContainerA = new KeeperContainerMeta().setId(1L).setIp("10.0.0.1").setPort(1).setDiskType("TFS").setTfsFsId("fs1");
        tfsContainerB = new KeeperContainerMeta().setId(2L).setIp("10.0.0.2").setPort(2).setDiskType("TFS").setTfsFsId("fs1");
        bmContainer = new KeeperContainerMeta().setId(3L).setIp("10.0.0.3").setPort(3).setDiskType("BM").setTfsFsId("");

        when(config.isOrphanGcEnabled()).thenReturn(true);
        when(config.isOrphanGcDryRun()).thenReturn(false);
        when(config.getOrphanGcMetaFreshMilli()).thenReturn(120_000L);
        when(currentClusterServer.isLeader()).thenReturn(true);
        when(metaCache.getLastSuccessfulLoadMilli()).thenReturn(System.currentTimeMillis());
        when(metaCache.getKeeperContainers()).thenReturn(Arrays.asList(tfsContainerA, tfsContainerB, bmContainer));
    }

    @After
    public void tearDown() {
        groupExecutor.shutdownNow();
    }

    private KeeperMeta keeperOn(KeeperContainerMeta container) {
        return new KeeperMeta().setIp(container.getIp()).setPort(container.getPort())
                .setKeeperContainerId(container.getId());
    }

    // ---- groupTfsContainersByFsId ----

    @Test
    public void testGroupTfsContainersByFsIdSkipsBmAndEmptyFsId() {
        KeeperContainerMeta noFsId = new KeeperContainerMeta().setId(4L).setIp("10.0.0.4").setPort(4).setDiskType("TFS").setTfsFsId("");

        Map<String, List<KeeperContainerMeta>> grouped = task.groupTfsContainersByFsId(
                Arrays.asList(tfsContainerA, tfsContainerB, bmContainer, noFsId));

        assertEquals(1, grouped.size());
        assertEquals(2, grouped.get("fs1").size());
        assertTrue(grouped.get("fs1").contains(tfsContainerA));
        assertTrue(grouped.get("fs1").contains(tfsContainerB));
    }

    // ---- indexContainersById ----

    @Test
    public void testIndexContainersById() {
        Map<Long, KeeperContainerMeta> byId = task.indexContainersById(Arrays.asList(tfsContainerA, tfsContainerB));

        assertEquals(2, byId.size());
        assertSame(tfsContainerA, byId.get(tfsContainerA.getId()));
        assertSame(tfsContainerB, byId.get(tfsContainerB.getId()));
    }

    // ---- occupiedReplIdsByFsId ----

    @Test
    public void testOccupiedReplIdsByFsIdCollectsShardDbIdsAcrossContainersInGroup() {
        KeeperMeta keeperOnA = keeperOn(tfsContainerA);
        KeeperMeta keeperOnB = keeperOn(tfsContainerB);
        KeeperMeta keeperOnBm = keeperOn(bmContainer);

        ShardMeta shard100 = new ShardMeta().setId("s100").setDbId(100L).addKeeper(keeperOnA);
        ShardMeta shard200 = new ShardMeta().setId("s200").setDbId(200L).addKeeper(keeperOnB);
        ShardMeta shardBm = new ShardMeta().setId("s300").setDbId(300L).addKeeper(keeperOnBm);
        ClusterMeta cluster = new ClusterMeta().setDbId(1L).addShard(shard100).addShard(shard200).addShard(shardBm);

        Set<ClusterMeta> clusters = new HashSet<>(Collections.singletonList(cluster));
        Map<Long, KeeperContainerMeta> containersById = task.indexContainersById(Arrays.asList(tfsContainerA, tfsContainerB, bmContainer));
        Map<String, Set<Long>> occupied = task.occupiedReplIdsByFsId(clusters, containersById);

        assertEquals(1, occupied.size());
        assertEquals(new HashSet<>(Arrays.asList(100L, 200L)), occupied.get("fs1"));
    }

    // ---- parseReplId ----

    @Test
    public void testParseReplId() {
        assertEquals(Long.valueOf(42L), task.parseReplId("repl_42"));
        assertNull(task.parseReplId("not_a_repl_dir"));
        assertNull(task.parseReplId("repl_notanumber"));
        assertNull(task.parseReplId(null));
    }

    // ---- doGcRound guards ----

    @Test
    public void testDoGcRoundSkipsWhenDisabled() {
        when(config.isOrphanGcEnabled()).thenReturn(false);

        task.doGcRound();

        verifyNoInteractions(keeperContainerServiceFactory);
    }

    @Test
    public void testDoGcRoundSkipsWhenNotLeader() {
        when(currentClusterServer.isLeader()).thenReturn(false);

        task.doGcRound();

        verifyNoInteractions(keeperContainerServiceFactory);
    }

    @Test
    public void testDoGcRoundSkipsWhenMetaStale() {
        when(metaCache.getLastSuccessfulLoadMilli()).thenReturn(System.currentTimeMillis() - 3_600_000L);

        task.doGcRound();

        verifyNoInteractions(keeperContainerServiceFactory);
    }

    @Test
    public void testDoGcRoundSkipsWhenMetaEmpty() {
        when(metaCache.getClusters()).thenReturn(Collections.emptySet());

        task.doGcRound();

        verifyNoInteractions(keeperContainerServiceFactory);
    }

    // ---- doGcRound end to end ----

    @Test
    public void testDoGcRoundGcsOnlyUnoccupiedCandidates() {
        KeeperMeta keeperOnA = keeperOn(tfsContainerA);
        ShardMeta occupiedShard = new ShardMeta().setId("s1").setDbId(1L).addKeeper(keeperOnA);
        ClusterMeta cluster = new ClusterMeta().setDbId(1L).addShard(occupiedShard);
        when(metaCache.getClusters()).thenReturn(new HashSet<>(Collections.singletonList(cluster)));

        KeeperContainerService serviceA = mock(KeeperContainerService.class);
        when(serviceA.listStoreDirNames()).thenReturn(Arrays.asList("repl_1", "repl_2", "not_a_repl_dir"));
        when(keeperContainerServiceFactory.getOrCreateKeeperContainerService(tfsContainerA)).thenReturn(serviceA);
        KeeperContainerService serviceB = mock(KeeperContainerService.class);
        when(keeperContainerServiceFactory.getOrCreateKeeperContainerService(tfsContainerB)).thenReturn(serviceB);

        task.doGcRound();

        // replId 1 is occupied -> excluded from the batch; replId 2 is not -> included, one batch call
        verify(serviceA, times(1)).gcOrphanStores(eq(Collections.singletonList(2L)), anyString());
    }

    @Test
    public void testDoGcRoundDryRunNeverCallsGc() {
        when(config.isOrphanGcDryRun()).thenReturn(true);
        ClusterMeta cluster = new ClusterMeta().setDbId(1L);
        when(metaCache.getClusters()).thenReturn(new HashSet<>(Collections.singletonList(cluster)));

        KeeperContainerService serviceA = mock(KeeperContainerService.class);
        when(serviceA.listStoreDirNames()).thenReturn(Collections.singletonList("repl_9"));
        when(keeperContainerServiceFactory.getOrCreateKeeperContainerService(tfsContainerA)).thenReturn(serviceA);
        when(keeperContainerServiceFactory.getOrCreateKeeperContainerService(tfsContainerB))
                .thenReturn(mock(KeeperContainerService.class));

        task.doGcRound();

        verify(serviceA, never()).gcOrphanStores(any(), anyString());
    }

    @Test
    public void testDoGcGroupFallsBackToNextContainerWhenFirstUnreachable() {
        ClusterMeta cluster = new ClusterMeta().setDbId(1L);
        when(metaCache.getClusters()).thenReturn(new HashSet<>(Collections.singletonList(cluster)));

        KeeperContainerService unreachable = mock(KeeperContainerService.class);
        when(unreachable.listStoreDirNames()).thenThrow(new RuntimeException("connect refused"));
        KeeperContainerService reachable = mock(KeeperContainerService.class);
        when(reachable.listStoreDirNames()).thenReturn(Collections.singletonList("repl_5"));
        when(reachable.gcOrphanStores(eq(Collections.singletonList(5L)), anyString()))
                .thenReturn(Collections.singletonList(GcOrphanStoreResult.deleted(5L)));

        when(keeperContainerServiceFactory.getOrCreateKeeperContainerService(tfsContainerA)).thenReturn(unreachable);
        when(keeperContainerServiceFactory.getOrCreateKeeperContainerService(tfsContainerB)).thenReturn(reachable);

        task.doGcRound();

        verify(reachable, times(1)).gcOrphanStores(eq(Collections.singletonList(5L)), anyString());
    }

    @Test
    public void testDoGcGroupSkipsBatchWhenLeadershipLostBeforeBatchCall() {
        ClusterMeta cluster = new ClusterMeta().setDbId(1L);
        when(metaCache.getClusters()).thenReturn(new HashSet<>(Collections.singletonList(cluster)));

        KeeperContainerService serviceA = mock(KeeperContainerService.class);
        when(serviceA.listStoreDirNames()).thenReturn(Collections.singletonList("repl_1"));
        when(keeperContainerServiceFactory.getOrCreateKeeperContainerService(tfsContainerA)).thenReturn(serviceA);
        when(keeperContainerServiceFactory.getOrCreateKeeperContainerService(tfsContainerB))
                .thenReturn(mock(KeeperContainerService.class));
        // Leader for the round-level checks, but gone by the time the group reaches its batch call.
        when(currentClusterServer.isLeader()).thenReturn(true, true, false);

        task.doGcRound();

        verify(serviceA, never()).gcOrphanStores(any(), anyString());
    }

}
