package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class CrdtPublishCommandTest extends AbstractTest {

    Server redis;

    private static final String testChannel = "test";

    private static final String testMessage = "hello";

    @Before
    public void setupCrdtPublishCommandTest() throws Exception {
        redis = startServer(":0\r\n");
    }

    @After
    public void afterCrdtPublishCommandTest() throws Exception {
        if (null != redis) redis.stop();
    }

    @Test
    public void testPublish() throws Exception {
        SimpleObjectPool<NettyClient> clientPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", redis.getPort()));
        PublishCommand crdtPublishCommand = new CRDTPublishCommand(clientPool, scheduled, 2000, testChannel, testMessage);
        AtomicBoolean pubSuccess = new AtomicBoolean(false);
        crdtPublishCommand.execute(executors).addListener(new CommandFutureListener<Object>() {

            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                pubSuccess.set(commandFuture.isSuccess());
            }

        });

        waitConditionUntilTimeOut(pubSuccess::get, 3000);
    }

}
