package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.BacklogOffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.XSyncContinue;
import com.ctrip.xpipe.redis.keeper.KeeperRepl;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperStats;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultKeeperStats;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;

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
    private RedisSlave slave;

    @Mock
    private KeeperConfig keeperConfig;

    private KeeperStats keeperStats;

    @Before
    public void setupGapAllowSyncHandlerTest() {
        keeperStats = new DefaultKeeperStats("repl_1", scheduled);
        Mockito.when(keeperServer.getKeeperRepl()).thenReturn(keeperRepl);
        Mockito.when(keeperServer.getKeeperMonitor()).thenReturn(keeperMonitor);
        Mockito.when(keeperMonitor.getKeeperStats()).thenReturn(keeperStats);
        Mockito.when(keeperConfig.getReplicationStoreMaxCommandsToTransferBeforeCreateRdb()).thenReturn(1000L);
    }

    @Test
    public void testXSyncAna_gapPartial() {
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(200L);
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.xsync("*", "A:1-10,B:1-15", 100);
        ReplStage replStage = new ReplStage("replid-test", 1, 1, "masterUuid-test", new GtidSet("C:1-5"), new GtidSet(""));
        XSyncContinue cont = new XSyncContinue(new GtidSet("A:1-10"), 100);
        GapAllowSyncHandler.SyncAction action = handler.anaXSync(request, replStage, cont, keeperRepl, keeperConfig, 200);

        Assert.assertFalse(action.isFull());
        Assert.assertFalse(action.protoSwitch);
        Assert.assertEquals(ReplStage.ReplProto.XSYNC, action.replStage.getProto());
        Assert.assertEquals(new GtidSet("A:1-10,C:1-5"), action.gtidSet);
        Assert.assertEquals(cont.getBacklogOffset(), action.backlogOffset);
        Assert.assertEquals(200, action.backlogEndOffsetExcluded);
        Assert.assertEquals(new GtidSet("B:1-15"), action.masterLost);
    }

    @Test
    public void testXSyncAna_gapFull() {
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(200L);
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.xsync("*", "A:1-10,B:1-15", 5);
        ReplStage replStage = new ReplStage("replid-test", 1, 1, "masterUuid-test", new GtidSet("C:1-5"), new GtidSet(""));
        XSyncContinue cont = new XSyncContinue(new GtidSet("A:1-10"), 100);
        GapAllowSyncHandler.SyncAction action = handler.anaXSync(request, replStage, cont, keeperRepl, keeperConfig, -1);
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
        GapAllowSyncHandler.SyncAction action = handler.anaPSync(request, replStage, keeperRepl, keeperConfig,500);
        Assert.assertFalse(action.full);
        Assert.assertEquals(250, action.backlogOffset);
        Assert.assertEquals("test-repl-id", action.replId);
        Assert.assertEquals(50, action.replOffset);
        Assert.assertEquals(500, action.backlogEndOffsetExcluded);
    }

    @Test
    public void testPSyncAna_full() {
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(300L);
        // wrong replId
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.psync("test-repl-id-wrong", 50);
        ReplStage replStage = new ReplStage("test-repl-id", 100, 201);
        Mockito.when(keeperRepl.backlogBeginOffset()).thenReturn(100L);
        GapAllowSyncHandler.SyncAction action = handler.anaPSync(request, replStage, keeperRepl, keeperConfig, -1);
        Assert.assertTrue(action.full);

        // repl offset miss
        request = GapAllowSyncHandler.SyncRequest.psync("test-repl-id", 1);
        action = handler.anaPSync(request, replStage, keeperRepl, keeperConfig, -1);
        Assert.assertTrue(action.full);

        // too much transfer
        request = GapAllowSyncHandler.SyncRequest.psync("test-repl-id", 100);
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(3000L);
        action = handler.anaPSync(request, replStage, keeperRepl, keeperConfig, -1);
        Assert.assertTrue(action.full);
    }

    @Test
    public void testXSyncAna_full() {
        Mockito.when(keeperRepl.backlogEndOffset()).thenReturn(3000L);
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.xsync("*", "A:1-10", 5);
        ReplStage replStage = new ReplStage("replid-test", 1, 1, "masterUuid-test", new GtidSet(""), new GtidSet(""));
        XSyncContinue cont = new XSyncContinue(new GtidSet("A:1-10"), 100);
        GapAllowSyncHandler.SyncAction action = handler.anaXSync(request, replStage, cont, keeperRepl, keeperConfig, -1);
        Assert.assertTrue(action.isFull());
    }

    @Test
    public void testXSyncAnaBacklogContSmall() {
        Mockito.when(keeperRepl.backlogBeginOffset()).thenReturn(90L);
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.xsync("*", "A:1-10", 5);
        ReplStage replStage = new ReplStage("replid-test", 1, 1, "masterUuid-test", new GtidSet(""), new GtidSet(""));
        XSyncContinue cont = new XSyncContinue(new GtidSet("A:1-10"), 100);
        GapAllowSyncHandler.SyncAction action = handler.anaXSync(request, replStage, cont, keeperRepl, keeperConfig, -1);
        Assert.assertFalse(action.isFull());

        ReplStage stage = new ReplStage("replid-test", 1, 101, "masterUuid-test", new GtidSet(""), new GtidSet(""));
        action = handler.anaXSync(request, stage, cont, keeperRepl, keeperConfig, -1);
        Assert.assertTrue(action.isFull());

        Mockito.when(keeperRepl.backlogBeginOffset()).thenReturn(101L);
        action = handler.anaXSync(request, replStage, cont, keeperRepl, keeperConfig, -1);
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
        Assert.assertNull(action.masterLost);
        Assert.assertEquals(1001, action.backlogOffset);
        Assert.assertEquals(-1, action.backlogEndOffsetExcluded);
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
        Mockito.when(keeperServer.locateContinueGtidSet(new GtidSet("C:1-15")))
                .thenReturn(new XSyncContinue(new GtidSet("B:1-5,C:1-15"), 1000));

        GapAllowSyncHandler.SyncRequest syncRequest = GapAllowSyncHandler.SyncRequest.psync("?", -2);
        GapAllowSyncHandler.SyncAction action = handler.anaRequest(syncRequest, keeperServer, slave);

        Assert.assertTrue(action.keeperPartial);
        Assert.assertFalse(action.full);
        Assert.assertEquals("test-repl-id1", action.replId);
        Assert.assertEquals(new GtidSet("A:1-10,B:1-5,C:1-15"), action.getGtidSet());
        Assert.assertEquals(new GtidSet("A:1-10"), action.keeperLost);
        Assert.assertEquals(1000, action.backlogOffset);
        Assert.assertEquals(-1, action.backlogEndOffsetExcluded);
        Assert.assertEquals(990, action.replOffset);
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
    }

}
