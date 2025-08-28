package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.FreshRdbOnlyGapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.RdbOnlyGapAllowedSync;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.core.store.UPDATE_RDB_RESULT;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperContextTest;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.exception.psync.GapAllowedSyncRdbNotContinuousRuntimeException;
import com.ctrip.xpipe.redis.keeper.exception.psync.PsyncCommandFailException;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.MasterStats;
import com.ctrip.xpipe.redis.keeper.monitor.ReplicationStoreStats;
import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class GapAllowedRdbonlyRedisMasterReplicationTest extends AbstractRedisKeeperContextTest {

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
    public void testRdbMoreRecent_dumpFailUntillWaitTimeout() throws  Exception {
        RedisMasterNewRdbDumper dumper = (RedisMasterNewRdbDumper)keeperRedisMaster.createRdbDumper(false, false);
        RdbonlyRedisMasterReplication gapAllowedRdbonlyRedisMasterReplication = new GapAllowedRdbonlyRedisMasterReplication(keeperServer,
                keeperRedisMaster, false, false, masterEventLoopGroup, scheduled, dumper, getRegistry().getComponent(KeeperResourceManager.class)) {
        };

        Psync psync = gapAllowedRdbonlyRedisMasterReplication.createPsync();
        Assert.assertTrue(psync instanceof RdbOnlyGapAllowedSync);

        psync.future().addListener(commandFuture -> {
            if (!commandFuture.isSuccess()) {
                gapAllowedRdbonlyRedisMasterReplication.dumpFail(new PsyncCommandFailException(commandFuture.cause()));
            }
        });

        when(replicationStore.checkReplIdAndUpdateRdbGapAllowed(any())).thenReturn(UPDATE_RDB_RESULT.RDB_MORE_RECENT);
        gapAllowedRdbonlyRedisMasterReplication.doRdbTypeConfirm(Mockito.mock(RdbStore.class));

        Assert.assertTrue(dumper.future().isDone());
        Assert.assertTrue(dumper.future().cause() instanceof IllegalStateException);
    }

    @Test
    public void testRdbMoreRecent_waitUntillCatchUp() throws Exception {
        RedisMasterNewRdbDumper dumper = (RedisMasterNewRdbDumper)keeperRedisMaster.createRdbDumper(false, false);
        RdbonlyRedisMasterReplication gapAllowedRdbonlyRedisMasterReplication = new GapAllowedRdbonlyRedisMasterReplication(keeperServer,
                keeperRedisMaster, false, false, masterEventLoopGroup, scheduled, dumper, getRegistry().getComponent(KeeperResourceManager.class));

        when(replicationStore.checkReplIdAndUpdateRdbGapAllowed(any()))
                .thenReturn(UPDATE_RDB_RESULT.RDB_MORE_RECENT)
                .thenReturn(UPDATE_RDB_RESULT.RDB_MORE_RECENT)
                .thenReturn(UPDATE_RDB_RESULT.OK);

        // expect no exception
        gapAllowedRdbonlyRedisMasterReplication.doRdbTypeConfirm(Mockito.mock(RdbStore.class));
    }

    @Test
    public void testRdbReplIdNotMatch_DumpFail() throws Exception {
        RedisMasterNewRdbDumper dumper = (RedisMasterNewRdbDumper)keeperRedisMaster.createRdbDumper(false, false);
        RdbonlyRedisMasterReplication gapAllowedRdbonlyRedisMasterReplication = new GapAllowedRdbonlyRedisMasterReplication(keeperServer,
                keeperRedisMaster, false, false, masterEventLoopGroup, scheduled, dumper, getRegistry().getComponent(KeeperResourceManager.class));

        Psync psync = gapAllowedRdbonlyRedisMasterReplication.createPsync();
        Assert.assertTrue(psync instanceof RdbOnlyGapAllowedSync);

        psync.future().addListener(commandFuture -> {
            if (!commandFuture.isSuccess()) {
                gapAllowedRdbonlyRedisMasterReplication.dumpFail(new PsyncCommandFailException(commandFuture.cause()));
            }
        });

        when(replicationStore.checkReplIdAndUpdateRdbGapAllowed(any())).thenReturn(UPDATE_RDB_RESULT.REPLID_NOT_MATCH);
        gapAllowedRdbonlyRedisMasterReplication.doRdbTypeConfirm(Mockito.mock(RdbStore.class));

        Assert.assertTrue(dumper.future().isDone());
        Assert.assertTrue(dumper.future().cause() instanceof IllegalStateException);
    }

    @Test
    public void testLackBacklog_retryFreshRdbonlyOnce() throws Exception {
        AtomicInteger reconnectCnt = new AtomicInteger(0);
        RedisMasterNewRdbDumper dumper = (RedisMasterNewRdbDumper)keeperRedisMaster.createRdbDumper(false, false);
        RdbonlyRedisMasterReplication gapAllowedRdbonlyRedisMasterReplication = new GapAllowedRdbonlyRedisMasterReplication(keeperServer,
                keeperRedisMaster, false, false, masterEventLoopGroup, scheduled, dumper, getRegistry().getComponent(KeeperResourceManager.class)) {
            @Override
            protected void scheduleReconnect(int timeMilli) {
                reconnectCnt.incrementAndGet();
            }
        };

        Psync psync = gapAllowedRdbonlyRedisMasterReplication.createPsync();
        Assert.assertTrue(psync instanceof RdbOnlyGapAllowedSync);

        psync.future().addListener(commandFuture -> {
            if (!commandFuture.isSuccess()) {
                gapAllowedRdbonlyRedisMasterReplication.dumpFail(new PsyncCommandFailException(commandFuture.cause()));
            }
        });

        when(replicationStore.checkReplIdAndUpdateRdbGapAllowed(any())).thenReturn(UPDATE_RDB_RESULT.LACK_BACKLOG);
        gapAllowedRdbonlyRedisMasterReplication.doRdbTypeConfirm(Mockito.mock(RdbStore.class));
        Assert.assertFalse(dumper.future().isDone());

        gapAllowedRdbonlyRedisMasterReplication.masterDisconnected(Mockito.mock(Channel.class));
        Assert.assertEquals(1, reconnectCnt.get());
        Assert.assertFalse(dumper.future().isDone());

        psync = gapAllowedRdbonlyRedisMasterReplication.createPsync();
        Assert.assertTrue(psync instanceof FreshRdbOnlyGapAllowedSync);

        // still lack backlog
        gapAllowedRdbonlyRedisMasterReplication.doRdbTypeConfirm(Mockito.mock(RdbStore.class));
        Assert.assertEquals(1, reconnectCnt.get());
        Assert.assertTrue(dumper.future().isDone());
        Assert.assertTrue(dumper.future().cause() instanceof GapAllowedSyncRdbNotContinuousRuntimeException);
    }

}