package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GapAllowPSyncHandlerTest extends AbstractTest {

    GapAllowPSyncHandler psyncHandler = new GapAllowPSyncHandler();

    @Mock
    private RedisSlave slave;

    @Before
    public void setupGapAllowXSyncHandlerTest() {
        psyncHandler = new GapAllowPSyncHandler();
    }

    @Test
    public void testXSyncParse() {
        String[] args = new String[] {"test-repl-id", "999"};
        GapAllowSyncHandler.SyncRequest request = psyncHandler.parseRequest(args, slave);
        Assert.assertEquals(ReplStage.ReplProto.PSYNC, request.proto);
        Assert.assertEquals("test-repl-id", request.replId);
        Assert.assertEquals(999, request.offset);
    }

}
