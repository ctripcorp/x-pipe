package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static com.ctrip.xpipe.AbstractTest.randomPort;

public class DeleteWrongSentinelsTest {

    private String monitorName = "shard1";
    private HostPort master = new HostPort("127.0.0.1", randomPort());
    private Set<HostPort> masterSentinels;

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
        DeleteWrongSentinels deleteWrongSentinels=new DeleteWrongSentinels(new SentinelHelloCollectContext(),null);
        Set<SentinelHello> hellos = Sets.newHashSet(

                new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5001), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5002), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5003), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5004), master, monitorName)

        );

        Set<SentinelHello> toDelete = deleteWrongSentinels.checkWrongHellos(monitorName, masterSentinels, hellos);

        Assert.assertEquals(0, toDelete.size());

        hellos.add(new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName + "_1"));
        toDelete = deleteWrongSentinels.checkWrongHellos(monitorName, masterSentinels, hellos);
        Assert.assertEquals(1, toDelete.size());
        Assert.assertEquals(5, hellos.size());

        hellos.add(new SentinelHello(new HostPort("127.0.0.1", 6000), master, monitorName));
        toDelete = deleteWrongSentinels.checkWrongHellos(monitorName, masterSentinels, hellos);
        Assert.assertEquals(1, toDelete.size());
        Assert.assertEquals(5, hellos.size());


    }
}
