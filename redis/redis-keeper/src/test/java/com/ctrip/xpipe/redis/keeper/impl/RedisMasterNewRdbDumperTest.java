package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.exception.psync.PsyncMasterRdbOffsetNotContinuousRuntimeException;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.*;

/**
 * @author Slight
 * <p>
 * Mar 08, 2021 4:38 PM
 */
public class RedisMasterNewRdbDumperTest {

    @Test
    public void whenRdbOffsetNotContinuous() throws Exception {
        RedisMaster redisMaster = mock(RedisMaster.class);
        doNothing().when(redisMaster).reconnect();

        ReplicationStoreManager storeManager = mock(ReplicationStoreManager.class);
        doReturn(mock(ReplicationStore.class)).when(storeManager).create();

        RedisKeeperServer redisKeeperServer = mock(RedisKeeperServer.class);
        doNothing().when(redisKeeperServer).closeSlaves(anyString());

        doReturn(storeManager).when(redisMaster).getReplicationStoreManager();

        RedisMasterNewRdbDumper dumper = spy(new RedisMasterNewRdbDumper(redisMaster, redisKeeperServer, mock(NioEventLoopGroup.class),
                mock(ScheduledExecutorService.class), mock(KeeperResourceManager.class)));
        doNothing().when(dumper).startRdbOnlyReplication();

        dumper.execute();
        dumper.future().setFailure(new PsyncMasterRdbOffsetNotContinuousRuntimeException(10, 20));

        verify(storeManager, times(1)).create();
        verify(redisMaster, times(1)).reconnect();
        verify(redisKeeperServer, times(1)).closeSlaves(anyString());
    }

    @Test
    public void cancel() throws Exception {
        RedisMasterNewRdbDumper dumper = spy(new RedisMasterNewRdbDumper(mock(RedisMaster.class), mock(RedisKeeperServer.class), mock(NioEventLoopGroup.class),
                mock(ScheduledExecutorService.class), mock(KeeperResourceManager.class)));

        doNothing().when(dumper).doExecute();
        doNothing().when(dumper).releaseResource();

        dumper.execute();
        dumper.future().cancel(true);

        verify(dumper, times(1)).releaseResource();
    }
}