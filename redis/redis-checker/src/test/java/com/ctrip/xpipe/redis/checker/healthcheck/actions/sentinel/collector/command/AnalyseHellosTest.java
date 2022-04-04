package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Set;

import static com.ctrip.xpipe.AbstractTest.randomPort;


@RunWith(MockitoJUnitRunner.class)
public class AnalyseHellosTest {

    private AnalyseHellos analyseHellos;

    private QuorumConfig quorumConfig = new QuorumConfig(5, 3);
    private String monitorName = "shard1";
    private Set<HostPort> masterSentinels;
    private HostPort master = new HostPort("127.0.0.1", randomPort());


    @Mock
    private CheckerConfig checkerConfig;

    @Before
    public void init() {
        analyseHellos = new AnalyseHellos(new SentinelHelloCollectContext(), checkerConfig);
        masterSentinels = Sets.newHashSet(
                new HostPort("127.0.0.1", 5000),
                new HostPort("127.0.0.1", 5001),
                new HostPort("127.0.0.1", 5002),
                new HostPort("127.0.0.1", 5003),
                new HostPort("127.0.0.1", 5004)
        );
    }

    @Test
    public void testAdd() {

        Set<SentinelHello> hellos = Sets.newHashSet(
                new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName)
        );

        Set<SentinelHello> toAdd = analyseHellos.checkToAdd("cluster1", "shard1", monitorName, masterSentinels, hellos, master, quorumConfig);
        Assert.assertEquals(4, toAdd.size());


        quorumConfig.setTotal(3);
        toAdd = analyseHellos.checkToAdd("cluster1", "shard1", monitorName, masterSentinels, hellos, master, quorumConfig);
        Assert.assertEquals(2, toAdd.size());

    }


    @Test
    public void checkWrongHelloMastersTest() {
        Set<SentinelHello> hellos = Sets.newHashSet(
                new SentinelHello(new HostPort("127.0.0.1", 5000), new HostPort("127.0.0.3", 6379), monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5001), new HostPort("127.0.0.3", 6380), monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5002), new HostPort("127.0.0.3", 6379), monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5003), new HostPort("127.0.0.3", 6381), monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5004), new HostPort("127.0.0.3", 6379), monitorName)
        );

        HostPort trueMaster = new HostPort("127.0.0.3", 6379);
        Set<SentinelHello> wrongHellos = analyseHellos.checkWrongMasterHellos(hellos, trueMaster);
        Assert.assertEquals(3, hellos.size());
        Assert.assertEquals(2, wrongHellos.size());
    }

}
