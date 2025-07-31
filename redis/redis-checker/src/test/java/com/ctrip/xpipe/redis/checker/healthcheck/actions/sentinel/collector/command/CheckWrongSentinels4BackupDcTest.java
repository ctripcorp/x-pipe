package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static com.ctrip.xpipe.AbstractTest.randomPort;

public class CheckWrongSentinels4BackupDcTest {

    private String monitorName = "shard1";
    private HostPort master = new HostPort("127.0.0.1", randomPort());
    private Set<HostPort> masterSentinels;

    private static final String WRONG_OTHER_SHARD = "wrong-sentinel-of-other-shard";
    private static final String WRONG_OUT_OF_XPIPE = "wrong-sentinel-out-of-xpipe";

    @Before
    public void init() {
        masterSentinels = Sets.newHashSet(
                new HostPort("127.0.0.1", 5000),
                new HostPort("127.0.0.1", 5001),
                new HostPort("127.0.0.1", 5002),
                new HostPort("127.0.0.1", 5003),
                new HostPort("127.0.0.1", 5004)
        );
    }

    @Test
    public void testDelete() {
        CheckWrongSentinels4BackupDc checkWrongSentinels =new CheckWrongSentinels4BackupDc(new SentinelHelloCollectContext(),masterSentinels);
        Set<SentinelHello> hellos = Sets.newHashSet(

                new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5001), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5002), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5003), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5004), master, monitorName)

        );

        Map<String, Set<SentinelHello>> wrongHellos = checkWrongSentinels.checkWrongHellos(monitorName, masterSentinels, hellos);

        Assert.assertEquals(2, wrongHellos.size());
        Assert.assertEquals(0, wrongHellos.get(WRONG_OTHER_SHARD).size());
        Assert.assertEquals(0, wrongHellos.get(WRONG_OUT_OF_XPIPE).size());

        hellos.add(new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName + "_1"));
        wrongHellos = checkWrongSentinels.checkWrongHellos(monitorName, masterSentinels, hellos);

        Assert.assertEquals(1, wrongHellos.get(WRONG_OTHER_SHARD).size());
        Assert.assertEquals(0, wrongHellos.get(WRONG_OUT_OF_XPIPE).size());
        Assert.assertEquals(5, hellos.size());

        hellos.add(new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName + "_1"));
        hellos.add(new SentinelHello(new HostPort("127.0.0.1", 6000), master, monitorName));
        wrongHellos = checkWrongSentinels.checkWrongHellos(monitorName, masterSentinels, hellos);

        Assert.assertEquals(1, wrongHellos.get(WRONG_OTHER_SHARD).size());
        Assert.assertEquals(1, wrongHellos.get(WRONG_OUT_OF_XPIPE).size());
        Assert.assertEquals(5, hellos.size());


    }
}
