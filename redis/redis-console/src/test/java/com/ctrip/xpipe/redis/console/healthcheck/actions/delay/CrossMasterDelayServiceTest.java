package com.ctrip.xpipe.redis.console.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class CrossMasterDelayServiceTest {

    @InjectMocks
    private CrossMasterDelayService service;

    @Mock
    private ConsoleServiceManager consoleServiceManager;

    @Mock
    private RedisHealthCheckInstance instance;

    @Mock
    private HealthCheckAction action;

    private String clusterId = "cluster1", shardId = "shard1", dcId = FoundationService.DEFAULT.getDataCenter();

    private String remoteDcId = "remote-dc";

    @Before
    public void setupCrossMasterDelayServiceTest() {
        Mockito.when(consoleServiceManager.getCrossMasterDelay(remoteDcId, clusterId, shardId)).thenReturn(Collections.singletonMap(dcId, Pair.of(new HostPort(), 10L)));
        Mockito.when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo(remoteDcId, clusterId, shardId, new HostPort(), null, ClusterType.BI_DIRECTION));
        Mockito.when(action.getActionInstance()).thenReturn(instance);
    }

    @Test
    public void testOnAction() {
        service.onAction(new DelayActionContext(instance, 10L));
        Assert.assertEquals(Collections.singletonMap(remoteDcId, Pair.of(new HostPort(), 10L)), service.getPeerMasterDelayFromCurrentDc(clusterId, shardId));
        Assert.assertEquals(Collections.singletonMap(remoteDcId, Pair.of(new HostPort(), 10L)), service.getPeerMasterDelayFromSourceDc(dcId, clusterId, shardId));
    }

    @Test
    public void testGetMasterDelayFromRemoteDc() {
        Assert.assertEquals(Collections.singletonMap(dcId, Pair.of(new HostPort(), 10L)), service.getPeerMasterDelayFromSourceDc(remoteDcId, clusterId, shardId));
    }

    @Test
    public void testStopWatch() {
        testOnAction();
        service.stopWatch(action);
        Assert.assertEquals(Collections.EMPTY_MAP, service.getPeerMasterDelayFromCurrentDc(clusterId, shardId));

        Mockito.when(instance.getRedisInstanceInfo()).thenReturn(new DefaultRedisInstanceInfo(dcId, clusterId, shardId, new HostPort(), null, ClusterType.BI_DIRECTION));
        service.stopWatch(action);
        Assert.assertEquals(null, service.getPeerMasterDelayFromCurrentDc(clusterId, shardId));
    }

}
