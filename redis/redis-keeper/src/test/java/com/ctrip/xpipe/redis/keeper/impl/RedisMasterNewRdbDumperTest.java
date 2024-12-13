package com.ctrip.xpipe.redis.keeper.impl;

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