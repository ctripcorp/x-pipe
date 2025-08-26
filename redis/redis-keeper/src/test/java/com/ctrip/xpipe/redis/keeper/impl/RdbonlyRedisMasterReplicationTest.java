package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.MasterStats;
import com.ctrip.xpipe.redis.keeper.monitor.ReplicationStoreStats;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Jul 05, 2018
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class RdbonlyRedisMasterReplicationTest extends AbstractRedisKeeperContextTest {

    private RedisMaster keeperRedisMaster;

    private NioEventLoopGroup masterEventLoopGroup = new NioEventLoopGroup(1);

    private NioEventLoopGroup rdbEventLoopGroup = new NioEventLoopGroup(1);

    private NioEventLoopGroup masterConfigEventLoopGroup = new NioEventLoopGroup(1);

    private ReplicationStoreManager replicationStoreManager = createReplicationStoreManager();

    private DefaultEndPoint target;

    @Mock
    private DefaultRedisKeeperServer keeperServer;

    @Mock
    private KeeperMonitor keeperMonitor;

    @Mock
    private MasterStats masterStats;

    @Mock
    private ReplicationStoreStats replicationStoreStats;

    @Mock
    private ReplicationStore replicationStore;

    @Before
    public void beforeRdbonlyRedisMasterReplicationTest() throws IOException {
        when(keeperServer.getRedisMaster()).thenReturn(keeperRedisMaster);
        when(keeperServer.getKeeperMonitor()).thenReturn(keeperMonitor);
        when(keeperMonitor.getMasterStats()).thenReturn(masterStats);
        when(keeperMonitor.getReplicationStoreStats()).thenReturn(replicationStoreStats);
        when(keeperServer.getReplicationStore()).thenReturn(replicationStore);

        target = new DefaultEndPoint("localhost", randomPort());
        this.keeperRedisMaster = new DefaultRedisMaster(keeperServer, target, masterEventLoopGroup, rdbEventLoopGroup, masterConfigEventLoopGroup,
                replicationStoreManager, scheduled, getRegistry().getComponent(KeeperResourceManager.class));
        keeperRedisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
    }

    @Test
    public void testTimeoutMilli() throws CreateRdbDumperException {
        RedisMasterNewRdbDumper dumper = (RedisMasterNewRdbDumper)keeperRedisMaster.createRdbDumper(false, false);
        RdbonlyRedisMasterReplication rdbonlyRedisMasterReplication = new RdbonlyRedisMasterReplication(keeperServer,
                keeperRedisMaster, false, false, masterEventLoopGroup, scheduled, dumper, getRegistry().getComponent(KeeperResourceManager.class));

        int time = rdbonlyRedisMasterReplication.commandTimeoutMilli();
        Assert.assertEquals(AbstractRedisCommand.PROXYED_REDIS_CONNECTION_COMMAND_TIME_OUT_MILLI, time);
    }

    @Test
    public void releaseRdbFileWhenCannotPsync() throws Exception {
        DumpedRdbStore rdbStore = mock(DumpedRdbStore.class);

        RdbonlyRedisMasterReplication replication = spy(new RdbonlyRedisMasterReplication(
                mock(DefaultRedisKeeperServer.class),
                mock(RedisMaster.class),
                false, false,
                mock(NioEventLoopGroup.class),
                mock(ScheduledExecutorService.class),
                mock(RedisMasterNewRdbDumper.class),
                mock(KeeperResourceManager.class)
        ));
        replication.dumpedRdbStore = rdbStore;
        doReturn(false).when(replication).canSendPsync();
        replication.sendReplicationCommand();

        verify(rdbStore, times(1)).close();
        verify(rdbStore, times(1)).destroy();
    }

}