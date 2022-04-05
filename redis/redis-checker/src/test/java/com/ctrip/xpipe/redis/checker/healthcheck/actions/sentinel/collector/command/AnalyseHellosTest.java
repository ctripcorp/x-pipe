package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.exception.SentinelsException;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


@RunWith(MockitoJUnitRunner.class)
public class AnalyseHellosTest extends AbstractCheckerTest {

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
    public void executeTest() throws Exception {
        Mockito.when(checkerConfig.getDefaultSentinelQuorumConfig()).thenReturn(quorumConfig);
        SentinelHelloCollectContext context = new SentinelHelloCollectContext();
        context.setSentinels(masterSentinels);

        SentinelHello hello_5000_wrong = new SentinelHello(new HostPort("127.0.0.1", 5000), new HostPort("127.0.0.1", 6379), monitorName);
        SentinelHello hello_5000_correct = new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName);

        SentinelHello hello_5001 = new SentinelHello(new HostPort("127.0.0.1", 5001), master, monitorName);
        SentinelHello hello_5002 = new SentinelHello(new HostPort("127.0.0.1", 5002), master, monitorName);
        SentinelHello hello_5003 = new SentinelHello(new HostPort("127.0.0.1", 5003), master, monitorName);
        SentinelHello hello_5004 = new SentinelHello(new HostPort("127.0.0.1", 5004), master, monitorName);

        Set<SentinelHello> hellos = Sets.newHashSet(hello_5000_wrong, hello_5001, hello_5002, hello_5003);
        context.setHellos(hellos);
        context.setSentinelMonitorName(monitorName);
        Map<HostPort, Throwable> networkSentinels = new HashMap<>();
        networkSentinels.put(new HostPort("127.0.0.1", 5003), new SentinelsException("test"));
        context.setNetworkErrorSentinels(networkSentinels);
        context.setTrueMasterInfo(new Pair<>(master, new ArrayList<>()));
        context.setInfo(newRandomRedisHealthCheckInstance(randomPort()).getCheckInfo());
        AnalyseHellos analyseHellos = new AnalyseHellos(context, checkerConfig);
        analyseHellos.execute().get();
        Assert.assertEquals(Sets.newHashSet(hello_5000_wrong), context.getToDelete());
        Assert.assertEquals(Sets.newHashSet(hello_5001, hello_5002), context.getToCheckReset());
        Assert.assertEquals(Sets.newHashSet(hello_5000_correct, hello_5004), context.getToAdd());
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
