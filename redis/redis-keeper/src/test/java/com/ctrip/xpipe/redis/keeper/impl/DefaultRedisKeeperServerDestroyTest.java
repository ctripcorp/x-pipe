package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;

import static org.mockito.Mockito.*;

/** destroy() decides destroy-vs-release: BM always destroys, TFS only when it held the store (ACTIVE/BACKUP). */
public class DefaultRedisKeeperServerDestroyTest {

    private final ReplicationStoreManager manager = mock(ReplicationStoreManager.class);

    private DefaultRedisKeeperServer server(boolean tfsMode, KeeperState keeperState) {
        ClusterMeta cluster = new ClusterMeta().setDbId(1L);
        ShardMeta shard = new ShardMeta().setDbId(1L);
        shard.setParent(cluster);
        KeeperMeta keeperMeta = new KeeperMeta().setIp("127.0.0.1").setPort(6000);
        keeperMeta.setParent(shard);

        DefaultRedisKeeperServer server = new DefaultRedisKeeperServer(1L, keeperMeta, new TestKeeperConfig(),
                new File("unused"), mock(LeaderElectorManager.class), mock(KeepersMonitorManager.class),
                mock(KeeperResourceManager.class), mock(SyncRateManager.class), mock(RedisOpParser.class),
                mock(AsyncFileSystem.class), null, tfsMode);
        server.setReplicationStoreManager(manager);
        if (keeperState != null) {
            RedisKeeperServerState state = mock(RedisKeeperServerState.class);
            when(state.keeperState()).thenReturn(keeperState);
            ReflectionTestUtils.setField(server, "redisKeeperServerState", state);
        }
        return server;
    }

    @Test
    public void testBmAlwaysDestroys() throws Exception {
        server(false, null).destroy();

        verify(manager, times(1)).destroy();
        verify(manager, never()).release();
    }

    @Test
    public void testTfsActiveDestroys() throws Exception {
        server(true, KeeperState.ACTIVE).destroy();

        verify(manager, times(1)).destroy();
        verify(manager, never()).release();
    }

    @Test
    public void testTfsBackupDestroys() throws Exception {
        server(true, KeeperState.BACKUP).destroy();

        verify(manager, times(1)).destroy();
        verify(manager, never()).release();
    }

    @Test
    public void testTfsPrepareReleasesInstead() throws Exception {
        server(true, KeeperState.PREPARE).destroy();

        verify(manager, times(1)).release();
        verify(manager, never()).destroy();
    }

    @Test
    public void testTfsUnknownStateReleasesInstead() throws Exception {
        server(true, null).destroy();

        verify(manager, times(1)).release();
        verify(manager, never()).destroy();
    }
}
