package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

/**
 * @author chen.zhu
 * <p>
 * Apr 09, 2018
 */
public class PublishCommandTest extends AbstractRedisTest {

    private PublishCommand publishCommand;

    private String channel = "hello";

    private String message = "world";

    @Before
    public void before() throws Exception {
        int port = randomPort();
        publishCommand = new PublishCommand(getXpipeNettyClientKeyedObjectPool()
                .getKeyPool(new DefaultEndPoint("127.0.0.1", port)), scheduled, channel, message);
        startEchoServer(port, "+OK");
    }


    @Test
    public void testPublishCommand() throws ExecutionException, InterruptedException {
        publishCommand.future().addListener(commandFuture -> {
            Assert.assertEquals("+OK", commandFuture.get());
        });
        publishCommand.execute().get();
    }

    @Test
    public void testPublishCommandManully() throws Exception {
        publishCommand = new PublishCommand(getXpipeNettyClientKeyedObjectPool()
                .getKeyPool(new DefaultEndPoint("127.0.0.1", 6379)), scheduled, channel, message);
        for(int i = 0; i < 100; i ++) {
            publishCommand.execute().get();
            Thread.sleep(1000);
        }


    }
}