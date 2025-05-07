package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GapAllowXSyncHandlerTest extends AbstractTest {

    GapAllowXSyncHandler xsyncHandler;

    @Mock
    private RedisSlave slave;

    @Before
    public void setupGapAllowXSyncHandlerTest() {
        xsyncHandler = new GapAllowXSyncHandler();
    }

    @Test
    public void testXSyncParse() {
        String[] args = new String[] {"*", "A:1-10", "MAXGAP", "100"};
        GapAllowSyncHandler.SyncRequest request = xsyncHandler.parseRequest(args, slave);
        Assert.assertEquals(ReplStage.ReplProto.XSYNC, request.proto);
        Assert.assertEquals("*", request.replId);
        Assert.assertEquals(new GtidSet("A:1-10"), request.slaveGtidSet);
        Assert.assertEquals(100, request.maxGap);
    }

}
