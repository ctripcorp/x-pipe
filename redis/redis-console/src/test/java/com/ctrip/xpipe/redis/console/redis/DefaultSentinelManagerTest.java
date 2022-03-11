package com.ctrip.xpipe.redis.console.redis;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.monitor.SentinelMonitors;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardDeleteEvent;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author chen.zhu
 * <p>
 * Feb 11, 2018
 */
public class DefaultSentinelManagerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DefaultSentinelManager manager;

    private int port;


    @Before
    public void beforeShardDeleteEventListener4SentinelTest() throws Exception {
        MockitoAnnotations.initMocks(this);
        manager.setScheduled(Executors.newScheduledThreadPool(5));
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
        manager.handleShardDelete(shardEvent);
    }

    @Test
    public void getRealSentinels() throws Exception {
        List<Sentinel> sentinelList = manager
                .getRealSentinels(Lists.newArrayList(new InetSocketAddress("127.0.0.1", port)), "sitemon-xpipegroup0");
        logger.info("{}", sentinelList);
    }

    @Test
    public void removeSentinel() throws Exception {
        try {
            logger.info("removeSentinel: {}", manager.removeSentinelMonitor(new Sentinel("test", "10.2.27.97", 5000), "credis_trocks_test+credis_trocks_test_1+TROCKS").execute().get(1000, TimeUnit.MILLISECONDS));
        }catch (Exception e){
            logger.error("removeSentinel failed",e);
        }
    }

    @Test// manual test
    public void infoSentinel() throws Exception {
        String info = manager.infoSentinel(new Sentinel("test", "10.2.27.97", 5000)).execute().get(1000, TimeUnit.MILLISECONDS);
        logger.info("=====================================");
        SentinelMonitors.parseFromString(info).getMonitors().forEach(monitor -> logger.info(monitor));
        logger.info("=====================================");

    }

    @Test// manual test
    public void sentinelSet() throws Exception {
        manager.sentinelSet(new Sentinel("test", "10.2.27.97", 5000), "credis_trocks_test+credis_trocks_test_1+TROCKS", new String[]{"failover-timeout", "60000"}).execute().get(1000, TimeUnit.MILLISECONDS);

        logger.info("sentinel set success");
    }

    @Test// manual test
    public void sentinelSlaves() throws Exception {
        List<HostPort> slaves = manager.slaves(new Sentinel("test", "10.2.27.97", 5000), "credis_trocks_test+credis_trocks_test_1+TROCKS").execute().get(3000, TimeUnit.MILLISECONDS);
        logger.info("sentinel slaves: {}", slaves);
    }

    @Test
    public void sentinelMonitor() throws Exception {
        try {
            logger.info("sentinelMonitor:{}", manager.monitorMaster(new Sentinel("test", "10.2.27.97", 5000), "credis_trocks_test+credis_trocks_test_1+TROCKS", new HostPort("10.2.27.55", 6399), 3).execute().get(1000, TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            logger.error("sentinelMonitor failed", e);
        }
    }

}