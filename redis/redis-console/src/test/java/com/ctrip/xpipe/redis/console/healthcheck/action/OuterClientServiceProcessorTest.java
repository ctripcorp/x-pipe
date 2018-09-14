package com.ctrip.xpipe.redis.console.healthcheck.action;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.factory.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 08, 2018
 */
public class OuterClientServiceProcessorTest extends AbstractRedisTest {

    @InjectMocks
    private OuterClientServiceProcessor processor = new OuterClientServiceProcessor();

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private MetaCache metaCache;

    @Mock
    private AlertManager alertManager;


    private String dc = "dc", cluster = "cluster", shard = "shard";

    private HostPort hostPort = localHostport(randomPort());

    @Before
    public void beforeOuterClientServiceProcessorTest() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testOnEvent() throws HealthEventProcessorException {
        RedisHealthCheckInstance instance = mock(RedisHealthCheckInstance.class);
        when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo(dc, cluster, shard, hostPort));
        when(consoleConfig.getDelayWontMarkDownClusters()).thenReturn(Sets.newHashSet(new Pair<>(dc, cluster)));
        when(metaCache.inBackupDc(hostPort)).thenReturn(true);
        processor.onEvent(new InstanceDown(instance));
        verify(alertManager, atLeastOnce()).alert(cluster, shard, hostPort, ALERT_TYPE.INSTANCE_LAG_NOT_MARK_DOWN, dc);
    }

}