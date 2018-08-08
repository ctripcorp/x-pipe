package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Test;

import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * Apr 08, 2018
 */
public class SubscribeCommandTest extends AbstractRedisTest {

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

        command.execute();

        while(!Thread.interrupted()) {
            Thread.sleep(1000);
        }
    }



}