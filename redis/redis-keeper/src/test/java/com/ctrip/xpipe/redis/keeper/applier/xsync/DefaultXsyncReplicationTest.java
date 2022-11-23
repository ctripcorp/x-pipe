package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.Xsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.core.server.FakeXsyncServer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;

/**
 * @author: cchen6
 * 2022/10/28
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultXsyncReplicationTest extends AbstractRedisTest {

    FakeXsyncServer server;

    @Test
    public void doDisconnect() throws Exception {
        server = startFakeXsyncServer(randomPort(), null);

        Xsync xsync = new DefaultXsync("127.0.0.1", server.getPort(), new GtidSet(""), null, scheduled);
        xsync.execute();

        Field nettyClientField = xsync.getClass().getDeclaredField("nettyClient");
        nettyClientField.setAccessible(true);
        NettyClient nettyClient = (NettyClient) nettyClientField.get(xsync);
        waitConditionUntilTimeOut(() -> nettyClient.channel().isActive());

        xsync.close();

        waitConditionUntilTimeOut(() -> !nettyClient.channel().isActive(), 3000);
    }
}