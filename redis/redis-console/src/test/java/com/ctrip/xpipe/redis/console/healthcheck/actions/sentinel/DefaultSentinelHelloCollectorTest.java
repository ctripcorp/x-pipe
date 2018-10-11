package com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.impl.HealthCheckEndpointFactory;
import com.ctrip.xpipe.redis.console.healthcheck.session.DefaultRedisSessionManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class DefaultSentinelHelloCollectorTest extends AbstractConsoleTest {

    private DefaultSentinelHelloCollector sentinelCollector;
    private QuorumConfig quorumConfig = new QuorumConfig(5, 3);
    private String monitorName = "shard1";
    private Set<HostPort> masterSentinels;
    private HostPort master = new HostPort("127.00.1", 6379);


    @Before
    public void beforeDefaultSentinelCollectorTest() throws Exception {
        sentinelCollector = new DefaultSentinelHelloCollector();
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

    @Test
    public void testCorrectWhenDR() throws Exception {
        MetaCache metaCache = mock(MetaCache.class);
        when(metaCache.inBackupDc(any(HostPort.class))).thenReturn(false);
        sentinelCollector.setMetaCache(metaCache);
        sentinelCollector.setConsoleConfig(mock(ConsoleConfig.class));
        sentinelCollector = spy(sentinelCollector);
        doCallRealMethod().when(sentinelCollector).onAction(any(SentinelActionContext.class));
        doReturn(null).when(sentinelCollector).checkAndDelete(anyString(), any(), any(), any());
        doNothing().when(sentinelCollector).checkReset(anyString(), any(), any(), any());
        doReturn(null).when(sentinelCollector).checkToAdd(anyString(), any(), any(), any(), any(), any(), any());
//        doNothing().when(sentinelCollector).doAction(any(), any(), any());
        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());
        Set<SentinelHello> hellos = Sets.newHashSet();
        for(int i = 0; i < 5; i++) {
            hellos.add(SentinelHello.fromString(String.format("127.0.0.1,%d,d156c06308a5e5c6edba1f8786b32e22cfceafcc,8410,shard,127.0.0.1,16379,0", 500 + i)));
        }
        sentinelCollector.onAction(new SentinelActionContext(instance, hellos));
        verify(sentinelCollector, never()).checkAndDelete(anyString(), any(), any(), any());
    }
}