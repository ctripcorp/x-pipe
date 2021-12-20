package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.proxy.ProxyEnabledEndpoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.proxy.protocols.DefaultProxyConnectProtocol;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Jul 05, 2018
 */
@RunWith(MockitoJUnitRunner.class)
public class RdbonlyRedisMasterReplicationTest extends AbstractRedisKeeperTest {

    private RedisMaster keeperRedisMaster;

    private NioEventLoopGroup masterEventLoopGroup = new NioEventLoopGroup(1);

    private NioEventLoopGroup rdbEventLoopGroup = new NioEventLoopGroup(1);

    private ReplicationStoreManager replicationStoreManager = createReplicationStoreManager();

    private DefaultEndPoint target;

    @Mock
    private DefaultRedisKeeperServer keeperServer;

    @Before
    public void beforeRdbonlyRedisMasterReplicationTest() throws IOException {
        MockitoAnnotations.initMocks(this);
        when(keeperServer.getRedisMaster()).thenReturn(keeperRedisMaster);
    }

    @Test
    public void testTimeoutMilli() throws CreateRdbDumperException {
        target = new DefaultEndPoint("localhost", randomPort());
        this.keeperRedisMaster = new DefaultRedisMaster(keeperServer, target, masterEventLoopGroup, rdbEventLoopGroup, replicationStoreManager, scheduled, getRegistry().getComponent(KeeperResourceManager.class));
        keeperRedisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
        RedisMasterNewRdbDumper dumper = (RedisMasterNewRdbDumper)keeperRedisMaster.createRdbDumper();
        RdbonlyRedisMasterReplication rdbonlyRedisMasterReplication = new RdbonlyRedisMasterReplication(keeperServer,
                keeperRedisMaster, masterEventLoopGroup, scheduled, dumper, getRegistry().getComponent(KeeperResourceManager.class));

        int time = rdbonlyRedisMasterReplication.commandTimeoutMilli();
        Assert.assertEquals(AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI, time);
    }

    @Test
    public void testProxiedMasterTimeoutMilli() throws CreateRdbDumperException {
        target = new ProxyEnabledEndpoint("localhost", randomPort(), new DefaultProxyConnectProtocol(null));
        this.keeperRedisMaster = new DefaultRedisMaster(keeperServer, target, masterEventLoopGroup, rdbEventLoopGroup, replicationStoreManager, scheduled, getRegistry().getComponent(KeeperResourceManager.class));
        keeperRedisMaster.setMasterState(MASTER_STATE.REDIS_REPL_CONNECTED);
        RedisMasterNewRdbDumper dumper = (RedisMasterNewRdbDumper)keeperRedisMaster.createRdbDumper();
        RdbonlyRedisMasterReplication rdbonlyRedisMasterReplication = new RdbonlyRedisMasterReplication(keeperServer,
                keeperRedisMaster, masterEventLoopGroup, scheduled, dumper, mock(KeeperResourceManager.class));

        int time = rdbonlyRedisMasterReplication.commandTimeoutMilli();
        Assert.assertEquals(AbstractRedisMasterReplication.PROXYED_REPLICATION_COMMAND_TIMEOUT_MILLI, time);
    }

    @Test
    public void releaseRdbFileWhenCannotPsync() throws Exception {
        DumpedRdbStore rdbStore = mock(DumpedRdbStore.class);

        RdbonlyRedisMasterReplication replication = spy(new RdbonlyRedisMasterReplication(
                mock(DefaultRedisKeeperServer.class),
                mock(RedisMaster.class),
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