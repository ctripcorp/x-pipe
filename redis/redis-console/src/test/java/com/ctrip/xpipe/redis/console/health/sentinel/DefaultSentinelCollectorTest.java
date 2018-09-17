package com.ctrip.xpipe.redis.console.health.sentinel;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.health.DefaultRedisSessionManager;
import com.ctrip.xpipe.redis.console.healthcheck.factory.DefaultHealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.console.healthcheck.factory.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 19, 2017
 */
public class DefaultSentinelCollectorTest extends AbstractConsoleTest{

    private DefaultSentinelCollector sentinelCollector;
    private QuorumConfig quorumConfig = new QuorumConfig(5, 3);
    private String monitorName = "shard1";
    private Set<HostPort> masterSentinels;
    private HostPort master = new HostPort("127.00.1", 6379);


    @Before
    public void beforeDefaultSentinelCollectorTest() throws Exception {
        sentinelCollector = new DefaultSentinelCollector();
        HealthCheckEndpointFactory endpointFactory = mock(HealthCheckEndpointFactory.class);
        when(endpointFactory.getOrCreateEndpoint(any(HostPort.class))).thenAnswer(new Answer<Endpoint>() {
            @Override
            public Endpoint answer(InvocationOnMock invocation) throws Throwable {
                HostPort hostPort = invocation.getArgumentAt(0, HostPort.class);
                return new DefaultEndPoint(hostPort.getHost(), hostPort.getPort());
            }
        });
        sentinelCollector.setSessionManager(new DefaultRedisSessionManager()
                .setExecutors(executors).setScheduled(scheduled).setEndpointFactory(endpointFactory)
                .setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool()));
        masterSentinels = Sets.newHashSet(
                new HostPort("127.0.0.1", 5000),
                new HostPort("127.0.0.1", 5001),
                new HostPort("127.0.0.1", 5002),
                new HostPort("127.0.0.1", 5003),
                new HostPort("127.0.0.1", 5004)
        );

    }

    @Test
    public void testAdd(){

        Set<SentinelHello> hellos = Sets.newHashSet(
                new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName)
        );

        Set<SentinelHello> toAdd = sentinelCollector.checkToAdd("cluster1", "shard1", monitorName, masterSentinels, hellos, master, quorumConfig);
        Assert.assertEquals(4, toAdd.size());


        quorumConfig.setTotal(3);
        toAdd = sentinelCollector.checkToAdd("cluster1", "shard1", monitorName, masterSentinels, hellos, master, quorumConfig);
        Assert.assertEquals(2, toAdd.size());

    }

    @Test
    public void testDelete(){

        Set<SentinelHello> hellos = Sets.newHashSet(

                new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5001), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5002), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5003), master, monitorName),
                new SentinelHello(new HostPort("127.0.0.1", 5004), master, monitorName)

        );

        Set<SentinelHello> toDelete = sentinelCollector.checkAndDelete(monitorName, masterSentinels, hellos, quorumConfig);

        Assert.assertEquals(0, toDelete.size());

        hellos.add(new SentinelHello(new HostPort("127.0.0.1", 5000), master, monitorName + "_1"));
        toDelete = sentinelCollector.checkAndDelete(monitorName, masterSentinels, hellos, quorumConfig);
        Assert.assertEquals(1, toDelete.size());
        Assert.assertEquals(5, hellos.size());

        hellos.add(new SentinelHello(new HostPort("127.0.0.1", 6000), master, monitorName));
        toDelete = sentinelCollector.checkAndDelete(monitorName, masterSentinels, hellos, quorumConfig);
        Assert.assertEquals(1, toDelete.size());
        Assert.assertEquals(5, hellos.size());


    }

    @Test
    public void testIsKeeperOrDead() {
        boolean result = sentinelCollector.isKeeperOrDead(localHostport(0));
        logger.info("{}", result);
        Assert.assertTrue(result);
    }

}
