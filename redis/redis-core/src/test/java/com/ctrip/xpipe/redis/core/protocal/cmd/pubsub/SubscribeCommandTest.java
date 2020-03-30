package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Before;
import org.junit.Test;

/**
 * @author chen.zhu
 * <p>
 * Apr 08, 2018
 */
public class SubscribeCommandTest extends AbstractRedisTest {

    private SimpleObjectPool<NettyClient> clientPool;

    private Server server;

    @Before
    public void beforeSubscribeCommandTest() throws Exception {
        server = startEmptyServer();
        int port = 6379;
        clientPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint("127.0.0.1", port));
    }

    @Test
    public void testSubscribeManually() throws Exception {
        String channel = "hello";
        SubscribeCommand command = new SubscribeCommand("127.0.0.1", 6379, scheduled, channel);

        command.addChannelListener(new SubscribeListener() {
            @Override
            public void message(String channel, String message) {
                logger.info("[message] channel: {}, message: {}", channel, message);
            }
        });

        command.execute().sync();
    }

    @Test
    public void testSubscribeMultiChannel() throws Exception {
        String[] channel = new String[]{"+sdown", "-sdown", "+odown", "-odown", "+reboot", "+switch-master"};
        SubscribeCommand command = new SubscribeCommand("10.3.2.23", 6399, scheduled, channel);

        command.addChannelListener(new SubscribeListener() {
            @Override
            public void message(String channel, String message) {
                logger.info("[message] channel: {}, message: {}", channel, message);
            }
        });

        command.execute().sync();
        logger.info("{}", "sleep");
        sleep(1000000);
    }


    @Test
    public void testUnSubscribe() throws Exception {
        SubscribeCommand command = new SubscribeCommand(clientPool, scheduled, "test");
        command.execute();
        Thread.sleep(1000);
        command.unSubscribe();
        Thread.sleep(2000);
    }

    @Test
    public void testUnSubscribeAndReSubscribe() throws Exception {
        int N = 10;
        for(int i = 0; i < N; i++) {
            SubscribeCommand command = new SubscribeCommand(clientPool, scheduled, "test");
            command.execute();
            Thread.sleep(1000);
            command.unSubscribe();
        }
        Thread.sleep(5000);
    }

    @Test
    public void testSubConnectTimeout() {
        SubscribeCommand command = new SubscribeCommand("127.0.0.1", 6379, scheduled, "test");
        command.addChannelListener(new SubscribeListener() {
            @Override
            public void message(String channel, String message) {

            }
        });
        command.future().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if(!commandFuture.isSuccess()) {
                    logger.error("[testSubConnectTimeout]", commandFuture.cause());
                }
            }
        });
        command.execute();
        sleep(15000);
    }
}