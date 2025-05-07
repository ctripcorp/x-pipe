package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.BacklogOffsetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.XSyncContinue;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
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
    private RedisSlave slave;

    @Before
    public void setupGapAllowSyncHandlerTest() {
    }

    @Test
    public void testXSyncAna_gapPartial() {
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.xsync("*", "A:1-10,B:1-15", 100);
        ReplStage replStage = new ReplStage("replid-test", 1, 1, "masterUuid-test", new GtidSet("C:1-5"), new GtidSet(""));
        XSyncContinue cont = new XSyncContinue(new GtidSet("A:1-10"), 100);
        GapAllowSyncHandler.SyncAction action = handler.anaXSync(request, replStage, cont);

        Assert.assertFalse(action.isFull());
        Assert.assertFalse(action.protoSwitch);
        Assert.assertEquals(ReplStage.ReplProto.XSYNC, action.replStage.getProto());
        Assert.assertEquals(new GtidSet("A:1-10,C:1-5"), action.gtidSet);
        Assert.assertEquals(cont.getBacklogOffset(), action.backlogOffset);
        Assert.assertEquals(new GtidSet("B:1-15"), action.masterLost);
    }

    @Test
    public void testXSyncAna_gapFull() {
        GapAllowSyncHandler.SyncRequest request = GapAllowSyncHandler.SyncRequest.xsync("*", "A:1-10,B:1-15", 5);
        ReplStage replStage = new ReplStage("replid-test", 1, 1, "masterUuid-test", new GtidSet("C:1-5"), new GtidSet(""));
        XSyncContinue cont = new XSyncContinue(new GtidSet("A:1-10"), 100);
        GapAllowSyncHandler.SyncAction action = handler.anaXSync(request, replStage, cont);
        Assert.assertTrue(action.isFull());
    }

    @Test
    public void testXSyncAction_full() throws Exception {
        GapAllowSyncHandler.SyncAction action = GapAllowSyncHandler.SyncAction.full("test");
        handler.runAction(action, keeperServer, slave);
        Mockito.verify(keeperServer).fullSyncToSlave(any());
    }

    @Test
    public void testXSyncAction_partial() throws Exception {
        ReplStage replStage = new ReplStage("replid-test", 1, 1, "masterUuid-test", new GtidSet("C:1-5"), new GtidSet(""));
        GapAllowSyncHandler.SyncAction action = GapAllowSyncHandler.SyncAction.XContinue(replStage, false, new GtidSet("A:1-10"), 100, new GtidSet("B:1-5"));
        handler.runAction(action, keeperServer, slave);
        Mockito.verify(keeperServer).increaseLost(new GtidSet("B:1-5"), slave);
        Mockito.verify(slave).sendMessage(any(ByteBuf.class));
        Mockito.verify(slave).beginWriteCommands(new BacklogOffsetReplicationProgress(100));
    }

}
