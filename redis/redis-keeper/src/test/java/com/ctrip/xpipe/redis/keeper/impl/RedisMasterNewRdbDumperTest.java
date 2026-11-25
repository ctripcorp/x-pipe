package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.exception.psync.PsyncMasterRdbOffsetNotContinuousRuntimeException;
import com.ctrip.xpipe.redis.keeper.store.DefaultReplicationStore;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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

        RedisKeeperServer redisKeeperServer = mock(RedisKeeperServer.class);
        doNothing().when(redisKeeperServer).resetDefaultReplication();

        RedisMasterNewRdbDumper dumper = spy(new RedisMasterNewRdbDumper(redisMaster, redisKeeperServer, false, false, mock(NioEventLoopGroup.class),
                mock(ScheduledExecutorService.class), mock(KeeperResourceManager.class)));
        doNothing().when(dumper).startRdbOnlyReplication();

        dumper.execute();
        dumper.future().setFailure(new PsyncMasterRdbOffsetNotContinuousRuntimeException(10, 20));

        verify(redisKeeperServer, times(1)).resetDefaultReplication();
    }

    @Test
    public void doExecuteFailMustDumpFailAndClearRdbDumper() throws Exception {
        RedisKeeperServer redisKeeperServer = mock(RedisKeeperServer.class);
        when(redisKeeperServer.slaves()).thenReturn(Collections.emptySet());

        RedisMasterNewRdbDumper dumper = spy(new RedisMasterNewRdbDumper(mock(RedisMaster.class), redisKeeperServer, false, false,
                mock(NioEventLoopGroup.class), mock(ScheduledExecutorService.class), mock(KeeperResourceManager.class)));
        doThrow(new IOException("prepareNewRdb fail")).when(dumper).startRdbOnlyReplication();

        dumper.execute();

        verify(dumper).dumpFail(any(IOException.class));
        verify(redisKeeperServer).clearRdbDumper(eq(dumper), eq(false));
        assertTrue(dumper.future().isDone());
        assertFalse(dumper.future().isSuccess());
    }

    @Test
    public void dumpFailDiscardsPreparedRdb() throws Exception {
        DefaultReplicationStore store = mock(DefaultReplicationStore.class);
        DumpedRdbStore dumped = mock(DumpedRdbStore.class);
        when(store.prepareNewRdb()).thenReturn(dumped);
        RedisMaster redisMaster = mock(RedisMaster.class);
        when(redisMaster.getCurrentReplicationStore()).thenReturn(store);

        RedisMasterNewRdbDumper dumper = spy(new RedisMasterNewRdbDumper(redisMaster, mock(RedisKeeperServer.class), false, false,
                mock(NioEventLoopGroup.class), mock(ScheduledExecutorService.class), mock(KeeperResourceManager.class)));
        doNothing().when(dumper).startRdbOnlyReplication();

        dumper.execute();
        dumper.prepareRdbStore();
        dumper.future().setFailure(new IOException("dump fail"));

        verify(store, times(1)).discardDumpedRdb(dumped);
    }

    @Test
    public void dumpSuccessKeepsPreparedRdb() throws Exception {
        DefaultReplicationStore store = mock(DefaultReplicationStore.class);
        when(store.prepareNewRdb()).thenReturn(mock(DumpedRdbStore.class));
        RedisMaster redisMaster = mock(RedisMaster.class);
        when(redisMaster.getCurrentReplicationStore()).thenReturn(store);

        RedisMasterNewRdbDumper dumper = spy(new RedisMasterNewRdbDumper(redisMaster, mock(RedisKeeperServer.class), false, false,
                mock(NioEventLoopGroup.class), mock(ScheduledExecutorService.class), mock(KeeperResourceManager.class)));
        doNothing().when(dumper).startRdbOnlyReplication();

        dumper.execute();
        dumper.prepareRdbStore();
        dumper.future().setSuccess();

        verify(store, never()).discardDumpedRdb(any());
    }

    @Test
    public void cancel() throws Exception {
        RedisMasterNewRdbDumper dumper = spy(new RedisMasterNewRdbDumper(mock(RedisMaster.class), mock(RedisKeeperServer.class), false, false, mock(NioEventLoopGroup.class),
                mock(ScheduledExecutorService.class), mock(KeeperResourceManager.class)));

        doNothing().when(dumper).doExecute();
        doNothing().when(dumper).releaseResource();

        dumper.execute();
        dumper.future().cancel(true);

        verify(dumper, times(1)).releaseResource();
    }
}