package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Set;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class CrossRegionSentinelHelloCollectorTest extends AbstractCheckerTest {

    @InjectMocks
    @Spy
    private CrossRegionSentinelHelloCollector sentinelCollector;

    private String monitorName = "shard1";
    private HostPort master = new HostPort("127.0.0.1", randomPort());

    @Mock
    private CheckerDbConfig checkerDbConfig;

    @Mock
    private MetaCache metaCache;

    @Test
    public void testCommand() throws Throwable {

        RedisHealthCheckInstance instance = newRandomRedisHealthCheckInstance(randomPort());

        Set<SentinelHello> sentinelHellos = Sets.newHashSet(
                new SentinelHello(new HostPort(LOCAL_HOST, 5000), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5001), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5002), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5003), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5004), master, monitorName),
                new SentinelHello(new HostPort(LOCAL_HOST, 5005), master, monitorName)
        );

        SentinelActionContext context = new SentinelActionContext(instance, sentinelHellos);
        when(metaCache.getAllSentinels()).thenReturn(Collections.singleton(master));

        DefaultSentinelHelloCollector.SentinelHelloCollectorCommand command = sentinelCollector.getCommand(context);
        verify(metaCache, times(1)).getAllSentinels();

        command = spy(command);
        Assert.assertTrue(command instanceof CrossRegionSentinelHelloCollector.SentinelHelloCollectorCommand4CrossRegion);

        command.execute().get();
        verify(command, times(1)).doExecute();
    }
}