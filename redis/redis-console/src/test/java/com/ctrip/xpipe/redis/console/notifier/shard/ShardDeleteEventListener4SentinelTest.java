package com.ctrip.xpipe.redis.console.notifier.shard;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * @author chen.zhu
 * <p>
 * Feb 09, 2018
 */
public class ShardDeleteEventListener4SentinelTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ShardDeleteEventListener4Sentinel listener;

    private int port;

    @Before
    public void beforeShardDeleteEventListener4SentinelTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        listener.setScheduled(Executors.newScheduledThreadPool(5));
        port = randomPort();
        startServer(port, new Function<String, String>() {
            @Override
            public String apply(String s) {
                if(s.contains("remove")) {
                    logger.info(s);
                    return "+OK\r\n";
                } else {
                    String result = "*1\r\n" +
                            "*6\r\n" +
                            "$4\r\n" +
                            "name\r\n" +
                            "$40\r\n" +
                            "b99ecc0cc2194c349c61bc2e95b59b9cb07250da\r\n" +
                            "$2\r\n" +
                            "ip\r\n" +
                            "$9\r\n" +
                            "127.0.0.1\n" +
                            "$4\r\n" +
                            "port\r\n" +
                            "$" + +Integer.toString(port).length() + "\r\n" +
                            port + "\r\n";
                    return result;
                }
            }
        });
    }

    @Test
    public void removeSentinels() throws Exception {
        ShardDeleteEvent shardEvent = new ShardDeleteEvent("cluster", "shard", Executors.newFixedThreadPool(2));
        shardEvent.setShardSentinels("127.0.0.1:"+port);
        shardEvent.setShardMonitorName("sitemon-xpipegroup0");
        listener.removeSentinels(shardEvent);
    }

    @Test
    public void getRealSentinels() throws Exception {
        List<Sentinel> sentinelList = listener
                .getRealSentinels(Lists.newArrayList(new InetSocketAddress("127.0.0.1", port)), "sitemon-xpipegroup0");
        logger.info("{}", sentinelList);
    }

    @Test
    public void removeSentinel() throws Exception {
        listener.removeSentinel(new Sentinel("b99ecc0cc2194c349c61bc2e95b59b9cb07250da", "127.0.0.1", port),
                "test");
    }

}