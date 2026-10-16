package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.store.BacklogOffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.XSyncContinue;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.impl.GapAllowRedisSlave;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultKeeperStats;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class GapAllowSyncHandlerTest extends AbstractTest {

    private GapAllowSyncHandler handler = new GapAllowSyncHandler() {
        @Override
        protected SyncRequest parseRequest(String[] args, RedisSlave redisSlave) {
            return null;
        }

        @Override
        public String[] getCommands() {
            return new String[0];
        }
    };

    @Mock
    private RedisKeeperServer keeperServer;

    @Mock
    private KeeperMonitor keeperMonitor;

    @Mock
    private KeeperRepl keeperRepl;

    @Mock
    private RedisClient<?> redisClient;

    @Mock
    private GapAllowRedisSlave gapAllowSlave;

    @Mock
    private RedisSlave slave;

    @Mock
    private KeeperConfig keeperConfig;

    @Mock
    private ReplicationStore store;

    @Mock
    private RedisKeeperServerState keeperServerState;

    private KeeperStats keeperStats;

    @Before
    public void setupGapAllowSyncHandlerTest() {
        keeperStats = new DefaultKeeperStats("repl_1", scheduled);
        Mockito.when(keeperServer.getKeeperRepl()).thenReturn(keeperRepl);
        Mockito.when(keeperServer.getKeeperMonitor()).thenReturn(keeperMonitor);
        Mockito.when(keeperMonitor.getKeeperStats()).thenReturn(keeperStats);
        Mockito.when(keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb()).thenReturn(1000L);
        Mockito.doReturn(keeperServer).when(redisClient).getRedisServer();
    }

    @Test
    public void testXSyncAna_gapPartial() {
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(200L);
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.xsync("*", "A:1-10,B:1-15", 100, "");
        ReplStage replStage = new ReplStage("replid-test", 1, 1, "masterUuid-test", new GtidSet("C:1-5"), new GtidSet(""));
        XSyncContinue cont = new XSyncContinue(new GtidSet("A:1-10"), 100);
        GapAllowSyncHandler.SyncAction action = handler.anaXSync(request, replStage, cont, keeperRepl, keeperConfig, true);

        Assert.assertFalse(action.isFull());
        Assert.assertFalse(action.protoSwitch);
        Assert.assertEquals(ReplStage.ReplProto.XSYNC, action.replStage.getProto());
        Assert.assertEquals(new GtidSet("A:1-10,C:1-5"), action.gtidSet);
        Assert.assertEquals(cont.getBacklogOffset(), action.backlogOffset);
        Assert.assertEquals(new GtidSet("B:1-15"), action.deltaLost);
    }

    @Test
    public void testXSyncAna_gapFull() {
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(200L);
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.xsync("*", "A:1-10,B:1-15", 5, "");
        ReplStage replStage = new ReplStage("replid-test", 1, 1, "masterUuid-test", new GtidSet("C:1-5"), new GtidSet(""));
        XSyncContinue cont = new XSyncContinue(new GtidSet("A:1-10"), 100);
        GapAllowSyncHandler.SyncAction action = handler.anaXSync(request, replStage, cont, keeperRepl, keeperConfig, true);
        Assert.assertTrue(action.isFull());
    }

    @Test
    public void testPSyncAna_partial() {
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.psync("test-repl-id2", 50);
        ReplStage replStage = new ReplStage("test-repl-id", 1, 201);
        replStage.setReplId2("test-repl-id2");
        replStage.setSecondReplIdOffset(100);
        Mockito.when(keeperRepl.backlogBeginOffset()).thenReturn(80L);
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(600L);
        GapAllowSyncHandler.SyncAction action = handler.anaPSync(request, replStage, keeperRepl, keeperConfig);
        Assert.assertFalse(action.full);
        Assert.assertEquals(250, action.backlogOffset);
        Assert.assertEquals("test-repl-id", action.replId);
        Assert.assertEquals(50, action.replOffset);
    }

    @Test
    public void testPSyncAna_full() {
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(300L);
        // wrong replId
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.psync("test-repl-id-wrong", 50);
        ReplStage replStage = new ReplStage("test-repl-id", 100, 201);
        Mockito.when(keeperRepl.backlogBeginOffset()).thenReturn(100L);
        GapAllowSyncHandler.SyncAction action = handler.anaPSync(request, replStage, keeperRepl, keeperConfig);
        Assert.assertTrue(action.full);

        // repl offset miss
        request = GapAllowSyncHandler.SyncRequest.psync("test-repl-id", 1);
        action = handler.anaPSync(request, replStage, keeperRepl, keeperConfig);
        Assert.assertTrue(action.full);

        // too much transfer
        request = GapAllowSyncHandler.SyncRequest.psync("test-repl-id", 100);
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(3000L);
        action = handler.anaPSync(request, replStage, keeperRepl, keeperConfig);
        Assert.assertTrue(action.full);
    }

    @Test
    public void testXSyncAna_full() {
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(3000L);
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.xsync("*", "A:1-10", 5, "");
        ReplStage replStage = new ReplStage("replid-test", 1, 1, "masterUuid-test", new GtidSet(""), new GtidSet(""));
        XSyncContinue cont = new XSyncContinue(new GtidSet("A:1-10"), 100);
        GapAllowSyncHandler.SyncAction action = handler.anaXSync(request, replStage, cont, keeperRepl, keeperConfig, true);
        Assert.assertTrue(action.isFull());
    }

    @Test
    public void testXSyncAnaBacklogContSmall() {
        Mockito.when(keeperRepl.backlogBeginOffset()).thenReturn(90L);
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.xsync("*", "A:1-10", 5, "");
        ReplStage replStage = new ReplStage("replid-test", 1, 1, "masterUuid-test", new GtidSet(""), new GtidSet(""));
        XSyncContinue cont = new XSyncContinue(new GtidSet("A:1-10"), 100);
        GapAllowSyncHandler.SyncAction action = handler.anaXSync(request, replStage, cont, keeperRepl, keeperConfig, true);
        Assert.assertFalse(action.isFull());

        ReplStage stage = new ReplStage("replid-test", 1, 101, "masterUuid-test", new GtidSet(""), new GtidSet(""));
        action = handler.anaXSync(request, stage, cont, keeperRepl, keeperConfig, true);
        Assert.assertTrue(action.isFull());

        Mockito.when(keeperRepl.backlogBeginOffset()).thenReturn(101L);
        action = handler.anaXSync(request, replStage, cont, keeperRepl, keeperConfig, true);
        Assert.assertTrue(action.isFull());

    }

    @Test
    public void testSyncAction_full() throws Exception {
        GapAllowSyncHandler.SyncAction action = GapAllowSyncHandler.SyncAction.full("test");
        handler.runAction(action, keeperServer, slave);
        Mockito.verify(keeperServer).fullSyncToSlave(any(), anyBoolean());
    }

    @Test
    public void testXSyncAction_partial() throws Exception {
        ReplStage replStage = new ReplStage("replid-test", 1, 1, "masterUuid-test", new GtidSet("C:1-5"), new GtidSet(""));
        GapAllowSyncHandler.SyncAction action = GapAllowSyncHandler.SyncAction.XContinue(replStage, new GtidSet("A:1-10"), 100, new GtidSet("B:1-5"))
                .setBacklogEndExcluded(200);
        handler.runAction(action, keeperServer, slave);
        Mockito.verify(keeperServer).increaseLost(new GtidSet("B:1-5"), slave);
        Mockito.verify(slave).sendMessage(any(ByteBuf.class));
        Mockito.verify(slave).beginWriteCommands(new BacklogOffsetReplicationProgress(100, 200));
    }

    @Test
    public void testPSyncAction_partial() throws Exception {
        ReplStage replStage = new ReplStage("test-repl-id", 1, 101);
        GapAllowSyncHandler.SyncAction action = GapAllowSyncHandler.SyncAction.Continue(replStage, "test-repl-id", 1000).setBacklogEndExcluded(2000);
        handler.runAction(action, keeperServer, slave);
        Mockito.verify(keeperServer, Mockito.never()).increaseLost(any(), any());
        Mockito.verify(slave).sendMessage(any(ByteBuf.class));
        Mockito.verify(slave).beginWriteCommands(new BacklogOffsetReplicationProgress(1100, 2000));
    }

    @Test
    public void testAnaPSync2XSync() throws Exception {
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.psync("test-repl-id2", 1001);
        ReplStage curStage = new ReplStage("test-repl-id", 1, 1001, "test-master-uuid", new GtidSet("A:1-10"), new GtidSet("A:1-20"));
        ReplStage preStage = new ReplStage("test-repl-id2", 1, 1);
        Mockito.when(keeperRepl.currentStage()).thenReturn(curStage);
        Mockito.when(keeperRepl.preStage()).thenReturn(preStage);

        GapAllowSyncHandler.SyncAction action = handler.anaRequest(request, keeperServer, slave);
        Assert.assertTrue(action.protoSwitch);
        Assert.assertEquals(new GtidSet("A:1-20"), action.getGtidSet());
        Assert.assertNull(action.deltaLost);
        Assert.assertEquals(1001, action.backlogOffset);
        Assert.assertEquals(-1, action.backlogEndOffsetExcluded);
    }

    @Test
    public void testPSync2XSync_butInconsistent() throws Exception {
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.psync("test-repl-id2", 2001);
        ReplStage curStage = new ReplStage("test-repl-id", 1, 1001, "test-master-uuid", new GtidSet("A:1-10"), new GtidSet("A:1-20"));
        ReplStage preStage = new ReplStage("test-repl-id2", 1, 1);
        Mockito.when(keeperRepl.currentStage()).thenReturn(curStage);
        Mockito.when(keeperRepl.preStage()).thenReturn(preStage);

        GapAllowSyncHandler.SyncAction action = handler.anaRequest(request, keeperServer, slave);
        Assert.assertTrue(action.full);
    }

    @Test
    public void testAnaXSync2PSync() throws Exception {

    }

    @Test
    public void testKeeperPartialSync_xcontinue() throws Exception {
        ReplStage replStage = new ReplStage("test-repl-id1", 1, 11, "C",
                new GtidSet("A:1-10"), new GtidSet("B:1-5"));
        Mockito.when(keeperRepl.currentStage()).thenReturn(replStage);
        Mockito.when(keeperRepl.getEndGtidSet()).thenReturn(new GtidSet("C:1-15"));
        Mockito.when(keeperServer.locateTailOfCmd())
                .thenReturn(new XSyncContinue(new GtidSet("B:1-5,C:1-15"), 1000));

        GapAllowSyncHandler.SyncRequest syncRequest = GapAllowSyncHandler.SyncRequest.psync("?", -2);
        GapAllowSyncHandler.SyncAction action = handler.anaRequest(syncRequest, keeperServer, slave);

        Assert.assertTrue(action.keeperPartial);
        Assert.assertFalse(action.full);
        Assert.assertEquals("test-repl-id1", action.replId);
        Assert.assertEquals(new GtidSet("A:1-10,B:1-5,C:1-15"), action.getGtidSet());
        Assert.assertEquals(new GtidSet("A:1-10"), action.gtidLost);
        Assert.assertEquals(1000, action.backlogOffset);
        Assert.assertEquals(-1, action.backlogEndOffsetExcluded);
        Assert.assertEquals(990, action.replOffset);
    }

    @Test
    public void testKeeperOffsetVerySmaller() throws Exception {
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.psync("test-repl-id2", 1);
        ReplStage curStage = new ReplStage("test-repl-id", 1, 1001, "test-master-uuid", new GtidSet("A:1-10"), new GtidSet("A:1-20"));
        ReplStage preStage = new ReplStage("test-repl-id2", 100, 1);
        Mockito.when(keeperRepl.currentStage()).thenReturn(curStage);
        Mockito.when(keeperRepl.preStage()).thenReturn(preStage);

        GapAllowSyncHandler.SyncAction action = handler.anaRequest(request, keeperServer, slave);
        Assert.assertTrue(action.full);
    }

    @Test
    public void testKeeperPartialSync_continue() throws Exception {
        ReplStage replStage = new ReplStage("test-repl-id1", 1, 11);
        Mockito.when(keeperRepl.currentStage()).thenReturn(replStage);
        Mockito.when(keeperRepl.getEndOffset()).thenReturn(1000L);

        GapAllowSyncHandler.SyncRequest syncRequest = GapAllowSyncHandler.SyncRequest.psync("?", -2);
        GapAllowSyncHandler.SyncAction action = handler.anaRequest(syncRequest, keeperServer, slave);

        Assert.assertFalse(action.full);
        Assert.assertTrue(action.keeperPartial);
        Assert.assertEquals("test-repl-id1", action.replId);
        Assert.assertEquals(1001, action.replOffset);
        Assert.assertEquals(1011, action.backlogOffset);
        Assert.assertEquals(-1, action.backlogEndOffsetExcluded);

        handler.runAction(action,keeperServer,slave);
        ArgumentCaptor<ByteBuf> byteBufCaptor = ArgumentCaptor.forClass(ByteBuf.class);

        Mockito.verify(slave).sendMessage(byteBufCaptor.capture());
        ByteBuf byteBuf = byteBufCaptor.getValue();
        byte[] offset = new byte[4];
        byteBuf.getBytes(byteBuf.readerIndex()+"+CONTINUE".length()+replStage.getReplId().length()+2,offset);
        String reploffStr = new String(offset);
        long replOffset = Long.parseLong(reploffStr);
        Assert.assertEquals(1001, replOffset);
    }

    @Test
    public void testAwaitOffset() throws Exception {
        ReplStage replStage = new ReplStage("test-repl-id1", 1, 1);
        Mockito.when(keeperServer.getKeeperRepl()).thenReturn(keeperRepl);
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(99L);
        Mockito.when(keeperServer.getReplicationStore()).thenReturn(store);
        Mockito.when(store.awaitCommandsOffset(anyLong(), anyInt())).thenReturn(true);

        GapAllowSyncHandler.SyncRequest syncRequest = GapAllowSyncHandler.SyncRequest.psync("test-repl-id1", 100);
        Assert.assertTrue(handler.awaitIfRequestExceedsCurrent(syncRequest, keeperServer, replStage, 3, 1));
        Mockito.verify(store, Mockito.times(0)).awaitCommandsOffset(Mockito.anyLong(), Mockito.anyInt());

        syncRequest = GapAllowSyncHandler.SyncRequest.psync("test-repl-id1", 200);
        Assert.assertTrue(handler.awaitIfRequestExceedsCurrent(syncRequest, keeperServer, replStage, 3, 1));
        Mockito.verify(store, Mockito.times(1)).awaitCommandsOffset(Mockito.anyLong(), Mockito.anyInt());

        Mockito.when(store.awaitCommandsOffset(anyLong(), anyInt())).thenReturn(false);
        Assert.assertFalse(handler.awaitIfRequestExceedsCurrent(syncRequest, keeperServer, replStage, 3, 1));
        Mockito.verify(store, Mockito.times(2)).awaitCommandsOffset(Mockito.anyLong(), Mockito.anyInt());
    }

    @Test
    public void testAwaitGtidset() throws Exception {
        ReplStage replStage = new ReplStage("test-repl-id1", 1, 1, "A", new GtidSet(""), new GtidSet(""));
        Mockito.when(keeperServer.getKeeperRepl()).thenReturn(keeperRepl);
        Mockito.when(keeperServer.getReplicationStore()).thenReturn(store);
        Mockito.when(store.getGtidSet()).thenReturn(new Pair<>(new GtidSet("A:1-10"), new GtidSet("")));

        GapAllowSyncHandler.SyncRequest syncRequest = GapAllowSyncHandler.SyncRequest.xsync("test-repl-id1", "A:1-10", 10, "");
        Assert.assertTrue(handler.awaitIfRequestExceedsCurrent(syncRequest, keeperServer, replStage, 3, 1));
        Mockito.verify(store, Mockito.times(1)).getGtidSet();

        syncRequest = GapAllowSyncHandler.SyncRequest.xsync("test-repl-id1", "A:1-15", 10, "");
        Assert.assertFalse(handler.awaitIfRequestExceedsCurrent(syncRequest, keeperServer, replStage, 3, 1));
        Mockito.verify(store, Mockito.times(5)).getGtidSet();
    }

    @Test
    public void testFullSync_lostNotIncreased() throws Exception {
        ReplStage replStage = new ReplStage("test-repl-id1", 1, 1, "A", new GtidSet(""), new GtidSet(""));
        Mockito.when(keeperServer.getKeeperRepl()).thenReturn(keeperRepl);
        Mockito.when(keeperServer.getReplicationStore()).thenReturn(store);
        Mockito.when(keeperServer.getKeeperConfig()).thenReturn(keeperConfig);
        Mockito.when(keeperRepl.currentStage()).thenReturn(replStage);
        Mockito.when(store.getGtidSet()).thenReturn(new Pair<>(new GtidSet("A:1-10"), new GtidSet("")));
        Mockito.when(keeperServer.locateContinueGtidSetWithFallbackToEnd(any(GtidSet.class))).thenReturn(new XSyncContinue(new GtidSet("A:1-5"), 100));

        GapAllowSyncHandler.SyncRequest syncRequest = GapAllowSyncHandler.SyncRequest.xsync("test-repl-id1", "A:1-5,B:1-5", 0, "");
        GapAllowSyncHandler.SyncAction action = handler.anaRequest(syncRequest, keeperServer, slave);
        Assert.assertTrue(action.full);

        handler.runAction(action, keeperServer, slave);
        Mockito.verify(keeperServer, Mockito.times(1)).fullSyncToSlave(slave, false);
        Mockito.verify(keeperServer, Mockito.never()).increaseLost(any(), any());
    }

    @Test
    public void testLostGap_partialSync() throws Exception {
        ReplStage replStage = new ReplStage("test-repl-id1", 1, 1, "A", new GtidSet(""), new GtidSet(""));
        Mockito.when(keeperServer.getKeeperRepl()).thenReturn(keeperRepl);
        Mockito.when(keeperServer.getReplicationStore()).thenReturn(store);
        Mockito.when(keeperServer.getKeeperConfig()).thenReturn(keeperConfig);
        Mockito.when(keeperRepl.currentStage()).thenReturn(replStage);
        Mockito.when(store.getGtidSet()).thenReturn(new Pair<>(new GtidSet("A:1-10"), new GtidSet("")));
        Mockito.when(keeperServer.locateContinueGtidSetWithFallbackToEnd(any(GtidSet.class))).thenReturn(new XSyncContinue(new GtidSet("A:1-5"), 100));

        GapAllowSyncHandler.SyncRequest syncRequest = GapAllowSyncHandler.SyncRequest.xsync("test-repl-id1", "A:1-5", 0, "B:1-5");
        GapAllowSyncHandler.SyncAction action = handler.anaRequest(syncRequest, keeperServer, slave);
        Assert.assertFalse(action.full);
        Assert.assertEquals(new GtidSet("B:1-5"), action.deltaLost);
    }

    @Test
    public void testKeeperLostGap0_FullSync() throws Exception {
        ReplStage replStage = new ReplStage("test-repl-id1", 1, 1, "A", new GtidSet("B:1-10"), new GtidSet(""));
        Mockito.when(keeperServer.getKeeperRepl()).thenReturn(keeperRepl);
        Mockito.when(keeperServer.getReplicationStore()).thenReturn(store);
        Mockito.when(keeperServer.getKeeperConfig()).thenReturn(keeperConfig);
        Mockito.when(keeperRepl.currentStage()).thenReturn(replStage);
        Mockito.when(store.getGtidSet()).thenReturn(new Pair<>(new GtidSet("A:1-10"), new GtidSet("B:1-10")));
        Mockito.when(keeperServer.locateContinueGtidSetWithFallbackToEnd(any(GtidSet.class))).thenReturn(new XSyncContinue(new GtidSet("A:1-5"), 100));

        GapAllowSyncHandler.SyncRequest syncRequest = GapAllowSyncHandler.SyncRequest.xsync("test-repl-id1", "A:1-5,B:6-10", 0, "B:1-5");
        GapAllowSyncHandler.SyncAction action = handler.anaRequest(syncRequest, keeperServer, slave);
        Assert.assertTrue(action.full);
    }

    @Test
    public void testEmptySlaveXsync_FullSync() throws Exception {
        ReplStage replStage = new ReplStage("test-repl-id1", 1, 1, "A", new GtidSet(""), new GtidSet(""));
        Mockito.when(keeperServer.getKeeperRepl()).thenReturn(keeperRepl);
        Mockito.when(keeperServer.getReplicationStore()).thenReturn(store);
        Mockito.when(keeperServer.getKeeperConfig()).thenReturn(keeperConfig);
        Mockito.when(keeperRepl.currentStage()).thenReturn(replStage);
        Mockito.when(store.getGtidSet()).thenReturn(new Pair<>(new GtidSet("cbd32d3e09d7d81574cc934b5c477312d9b03ca1:1-2191784636"), new GtidSet("")));
        Mockito.when(keeperServer.locateContinueGtidSetWithFallbackToEnd(any(GtidSet.class))).thenReturn(new XSyncContinue(new GtidSet("cbd32d3e09d7d81574cc934b5c477312d9b03ca1:1-2191784636"), 400509272434L));

        GapAllowSyncHandler.SyncRequest syncRequest = GapAllowSyncHandler.SyncRequest.xsync("*", "", 10000, "");
        GapAllowSyncHandler.SyncAction action = handler.anaRequest(syncRequest, keeperServer, slave);
        Assert.assertTrue(action.full);
        Assert.assertTrue(action.fullCause.startsWith("[gtid not related]"));

        syncRequest = GapAllowSyncHandler.SyncRequest.xsync("*", "cbd32d3e09d7d81574cc934b5c477312d9b03ca1:1", 10000, "");
        action = handler.anaRequest(syncRequest, keeperServer, slave);
        Assert.assertTrue(action.full);
        Assert.assertTrue(action.fullCause.startsWith("[gap]"));
    }

    @Test
    public void testCmdTailSync_psyncStage() throws Exception {
        ReplStage replStage = new ReplStage("test-repl-id1", 1, 11);
        Mockito.when(keeperRepl.currentStage()).thenReturn(replStage);
        Mockito.when(keeperRepl.getEndOffset()).thenReturn(1000L);

        GapAllowSyncHandler.SyncAction action = handler.anaRequest(cmdTailRequest(), keeperServer, slave);

        assertCmdTailContinue(action, "test-repl-id1", 1001, 1011);
        Mockito.verify(keeperServer, Mockito.never()).locateTailOfCmd();

        handler.runAction(action, keeperServer, slave);
        assertContinueReply("test-repl-id1", 1001);
        Mockito.verify(slave).beginWriteCommands(new BacklogOffsetReplicationProgress(1011, -1));
        Mockito.verify(slave).partialSync();
        Mockito.verify(keeperServer, Mockito.never()).fullSyncToSlave(any(), anyBoolean());
    }

    @Test
    public void testCmdTailSync_xsyncStageStillContinue() throws Exception {
        ReplStage replStage = new ReplStage("test-repl-id1", 1, 11, "C",
                new GtidSet("A:1-10"), new GtidSet("B:1-5"));
        Mockito.when(keeperRepl.currentStage()).thenReturn(replStage);
        Mockito.when(keeperRepl.getEndOffset()).thenReturn(2000L);

        GapAllowSyncHandler.SyncAction action = handler.anaRequest(cmdTailRequest(), keeperServer, slave);

        assertCmdTailContinue(action, "test-repl-id1", 2001, 2011);
        Assert.assertNull(action.gtidSet);
        Mockito.verify(keeperServer, Mockito.never()).locateTailOfCmd();

        handler.runAction(action, keeperServer, slave);
        assertContinueReply("test-repl-id1", 2001);
        Mockito.verify(slave).beginWriteCommands(new BacklogOffsetReplicationProgress(2011, -1));
        Mockito.verify(slave).partialSync();
        Mockito.verify(keeperServer, Mockito.never()).fullSyncToSlave(any(), anyBoolean());
    }

    @Test
    public void testFreshStoreErrorsOnCommandThreadWithoutBecomeSlave() throws Exception {
        Mockito.when(keeperServer.getRedisKeeperServerState()).thenReturn(keeperServerState);
        Mockito.when(keeperServerState.psync(redisClient, new String[]{"?", "-4"})).thenReturn(true);
        Mockito.when(keeperServer.getReplicationStore()).thenReturn(store);
        Mockito.when(store.isFresh()).thenReturn(true);

        handler.doHandle(new String[]{"?", "-4"}, redisClient);

        Mockito.verify(keeperServerState).psync(redisClient, new String[]{"?", "-4"});
        Mockito.verify(redisClient).sendMessage(any(ByteBuf.class));
        Mockito.verify(redisClient, Mockito.never()).becomeGapAllowRedisSlave();
    }

    @Test
    public void testNullStoreTreatedAsFreshOnCommandThread() throws Exception {
        Mockito.when(keeperServer.getRedisKeeperServerState()).thenReturn(keeperServerState);
        Mockito.when(keeperServerState.psync(redisClient, new String[]{"?", "-4"})).thenReturn(true);
        Mockito.when(keeperServer.getReplicationStore()).thenReturn(null);

        handler.doHandle(new String[]{"?", "-4"}, redisClient);

        Mockito.verify(keeperServerState).psync(redisClient, new String[]{"?", "-4"});
        Mockito.verify(redisClient).sendMessage(any(ByteBuf.class));
        Mockito.verify(redisClient, Mockito.never()).becomeGapAllowRedisSlave();
    }

    @Test
    public void testNonFreshStoreOpensOnCommandThreadThenPsyncExecutor() throws Exception {
        Mockito.when(keeperServer.getReplicationStore()).thenReturn(store);
        Mockito.when(store.isFresh()).thenReturn(false);
        Mockito.when(keeperServer.getRedisKeeperServerState()).thenReturn(keeperServerState);
        Mockito.when(keeperServerState.psync(redisClient, new String[]{"?", "-4"})).thenReturn(true);
        Mockito.when(redisClient.becomeGapAllowRedisSlave()).thenReturn(gapAllowSlave);
        Mockito.doAnswer(invocation -> {
            ((Runnable) invocation.getArgument(0)).run();
            return null;
        }).when(gapAllowSlave).processPsyncSequentially(any(Runnable.class));

        handler.doHandle(new String[]{"?", "-4"}, redisClient);

        Mockito.verify(keeperServer).getReplicationStore();
        Mockito.verify(store).isFresh();
        Mockito.verify(gapAllowSlave).processPsyncSequentially(any(Runnable.class));
    }

    @Test
    public void testFreshStoreErrorsAndClosesForAllRequestTypes() throws Exception {
        Mockito.when(keeperRepl.currentStage()).thenReturn(null);
        Mockito.when(keeperRepl.preStage()).thenReturn(null);

        GapAllowSyncHandler.SyncRequest[] requests = new GapAllowSyncHandler.SyncRequest[] {
                cmdTailRequest(),
                GapAllowSyncHandler.SyncRequest.psync("?", Psync.KEEPER_PARTIAL_SYNC_OFFSET),
                GapAllowSyncHandler.SyncRequest.psync("?", -1),
                GapAllowSyncHandler.SyncRequest.psync("?", Psync.KEEPER_FRESH_RDB_SYNC_OFFSET),
                GapAllowSyncHandler.SyncRequest.psync("some-repl", 1)
        };
        for (GapAllowSyncHandler.SyncRequest request : requests) {
            Mockito.clearInvocations(slave, keeperServer);
            GapAllowSyncHandler.SyncAction action = handler.anaRequest(request, keeperServer, slave);
            Assert.assertNull(request.toString(), action);
            Mockito.verify(slave).sendMessage(any(ByteBuf.class));
            Mockito.verify(slave).close();
            Mockito.verify(keeperServer, Mockito.never()).locateTailOfCmd();
            Mockito.verify(keeperServer, Mockito.never()).fullSyncToSlave(any(), anyBoolean());
        }
    }

    @Test
    public void testPrepareRejectsFullRequests() throws Exception {
        ReplStage replStage = new ReplStage("test-repl-id1", 1, 11);
        Mockito.when(keeperRepl.currentStage()).thenReturn(replStage);
        Mockito.when(keeperRepl.backlogBeginOffset()).thenReturn(11L);
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(100L);
        Mockito.when(keeperServer.getKeeperConfig()).thenReturn(keeperConfig);
        Mockito.when(keeperServer.getRedisKeeperServerState()).thenReturn(keeperServerState);
        Mockito.when(keeperServerState.keeperState()).thenReturn(KeeperState.PREPARE);

        GapAllowSyncHandler.SyncRequest[] fullRequests = new GapAllowSyncHandler.SyncRequest[] {
                GapAllowSyncHandler.SyncRequest.psync("?", -1),
                GapAllowSyncHandler.SyncRequest.psync("?", Psync.KEEPER_FRESH_RDB_SYNC_OFFSET),
                GapAllowSyncHandler.SyncRequest.psync("no-such-repl", 1)
        };
        for (GapAllowSyncHandler.SyncRequest request : fullRequests) {
            Mockito.clearInvocations(slave, keeperServer);
            GapAllowSyncHandler.SyncAction action = handler.anaRequest(request, keeperServer, slave);
            Assert.assertTrue(request.toString(), action.isFull());
            handler.runAction(action, keeperServer, slave);
            Mockito.verify(keeperServer, Mockito.never()).fullSyncToSlave(any(), anyBoolean());
            ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
            Mockito.verify(slave).sendMessage(captor.capture());
            Assert.assertTrue(toAscii(captor.getValue()).contains(GapAllowSyncHandler.PREPARE_PARTIAL_ONLY_ERROR));
            Mockito.verify(slave).close();
        }
    }

    private static GapAllowSyncHandler.SyncRequest cmdTailRequest() {
        return GapAllowSyncHandler.SyncRequest.psync("?", Psync.KEEPER_CMD_TAIL_SYNC_OFFSET);
    }

    private static void assertCmdTailContinue(GapAllowSyncHandler.SyncAction action, String replId,
                                              long replOffset, long backlogOffset) {
        Assert.assertFalse(action.isFull());
        Assert.assertTrue(action.keeperPartial);
        Assert.assertEquals(replId, action.replId);
        Assert.assertEquals(replOffset, action.replOffset);
        Assert.assertEquals(backlogOffset, action.backlogOffset);
        Assert.assertEquals(-1, action.backlogEndOffsetExcluded);
    }

    private void assertContinueReply(String replId, long offset) {
        ArgumentCaptor<ByteBuf> captor = ArgumentCaptor.forClass(ByteBuf.class);
        Mockito.verify(slave).sendMessage(captor.capture());
        String resp = toAscii(captor.getValue());
        Assert.assertTrue(resp, resp.startsWith("+CONTINUE "));
        Assert.assertFalse(resp, resp.contains("XCONTINUE"));
        Assert.assertTrue(resp, resp.contains(replId));
        Assert.assertTrue(resp, resp.contains(String.valueOf(offset)));
    }

    private static String toAscii(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), bytes);
        return new String(bytes);
    }

}
