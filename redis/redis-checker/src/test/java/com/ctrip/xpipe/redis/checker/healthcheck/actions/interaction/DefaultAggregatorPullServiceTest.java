package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2024/10/15
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultAggregatorPullServiceTest extends AbstractCheckerTest {

    @InjectMocks
    private DefaultAggregatorPullService aggregatorPullService;

    @Mock
    private OuterClientService outerClientService;

    @Mock
    private RemoteCheckerManager remoteCheckerManager;

    @Mock
    private CheckerService checkerService1;

    @Mock
    private CheckerService checkerService2;

    @Mock
    private MetaCache metaCache;

    @Mock
    private CheckerConfig config;

    private HostPort hostPort1 = new HostPort("127.0.0.1", 6379);
    private HostPort hostPort2 = new HostPort("127.0.0.1", 6380);

    @Before
    public void setupDefaultAggregatorPullServiceTest() throws Exception {
        aggregatorPullService.setExecutors(MoreExecutors.directExecutor());

        when(remoteCheckerManager.getAllCheckerServices()).thenReturn(Arrays.asList(checkerService1, checkerService2));
        when(config.getQuorum()).thenReturn(2);

        Map<HostPort, Boolean> outerClientState = new HashMap<>();
        outerClientState.put(hostPort1, true);
        outerClientState.put(hostPort2, false);
        when(outerClientService.batchQueryInstanceStatus(anyString(), anySet())).thenReturn(outerClientState);
        when(metaCache.getDc(any())).thenReturn("jq");
    }

    @Test
    public void testQuorumWithUndefined() throws Exception {
        Set<HostPort> instances = Sets.newHashSet(hostPort1, hostPort2);
        Map<HostPort, HealthStatusDesc> healthStatusDescMap = new HashMap<>();
        healthStatusDescMap.put(hostPort1, new HealthStatusDesc(hostPort1, HEALTH_STATE.HEALTHY));
        healthStatusDescMap.put(hostPort2, new HealthStatusDesc(hostPort1, HEALTH_STATE.UNKNOWN));

        when(checkerService1.getAllClusterInstanceHealthStatus(instances)).thenReturn(healthStatusDescMap);
        when(checkerService2.getAllClusterInstanceHealthStatus(instances)).thenReturn(healthStatusDescMap);

        DefaultAggregatorPullService.QueryXPipeInstanceStatusCmd cmd = aggregatorPullService.new QueryXPipeInstanceStatusCmd("cluster1", instances);
        XPipeInstanceHealthHolder holder = cmd.execute().get();
        Map<HostPort, Boolean> healthStatusMap = holder.getAllHealthStatus(2);
        Assert.assertEquals(2, healthStatusMap.size());
        Assert.assertTrue(healthStatusMap.get(hostPort1));
        Assert.assertNull(healthStatusMap.get(hostPort2));
    }

    @Test
    public void testQuorumWithEmpty() throws Exception {
        Set<HostPort> instances = Sets.newHashSet(hostPort1, hostPort2);
        Map<HostPort, HealthStatusDesc> healthStatusDescMap = new HashMap<>();
        healthStatusDescMap.put(hostPort1, new HealthStatusDesc(hostPort1, HEALTH_STATE.HEALTHY));
        healthStatusDescMap.put(hostPort2, new HealthStatusDesc(hostPort1, HEALTH_STATE.HEALTHY));

        when(checkerService1.getAllClusterInstanceHealthStatus(instances)).thenReturn(Collections.emptyMap());
        when(checkerService2.getAllClusterInstanceHealthStatus(instances)).thenReturn(healthStatusDescMap);

        DefaultAggregatorPullService.QueryXPipeInstanceStatusCmd cmd = aggregatorPullService.new QueryXPipeInstanceStatusCmd("cluster1", instances);
        XPipeInstanceHealthHolder holder = cmd.execute().get();
        Map<HostPort, Boolean> healthStatusMap = holder.getAllHealthStatus(2);
        Assert.assertEquals(2, healthStatusMap.size());
        Assert.assertNull(healthStatusMap.get(hostPort1));
        Assert.assertNull(healthStatusMap.get(hostPort2));
    }

    @Test
    public void testMarkAggregate() throws Exception {
        Set<HostPort> instances = Sets.newHashSet(hostPort1, hostPort2);
        Map<HostPort, HealthStatusDesc> healthStatusDescMap = new HashMap<>();
        healthStatusDescMap.put(hostPort1, new HealthStatusDesc(hostPort1, HEALTH_STATE.DOWN));
        healthStatusDescMap.put(hostPort2, new HealthStatusDesc(hostPort1, HEALTH_STATE.DOWN));
        when(checkerService1.getAllClusterInstanceHealthStatus(instances)).thenReturn(healthStatusDescMap);
        when(checkerService2.getAllClusterInstanceHealthStatus(instances)).thenReturn(healthStatusDescMap);

        Set<OuterClientService.HostPortDcStatus> hostPortDcStatuses = aggregatorPullService.getNeedAdjustInstances("cluster1", instances);
        Assert.assertEquals(1, hostPortDcStatuses.size());

        OuterClientService.HostPortDcStatus status = hostPortDcStatuses.iterator().next();
        Assert.assertEquals(hostPort1.getHost(), status.getHost());
        Assert.assertEquals(hostPort1.getPort(), status.getPort());
        Assert.assertEquals("jq", status.getDc());
        Assert.assertFalse(status.isCanRead());
    }

    @Test
    public void testQuorumWithLastMark() throws Exception {
        Set<HostPort> instances = Sets.newHashSet(hostPort1, hostPort2);
        Map<HostPort, HealthStatusDesc> healthStatusDescMap = new HashMap<>();
        healthStatusDescMap.put(hostPort1, new HealthStatusDesc(hostPort1, HEALTH_STATE.DOWN, false));
        healthStatusDescMap.put(hostPort2, new HealthStatusDesc(hostPort1, HEALTH_STATE.HEALTHY, false));
        when(checkerService1.getAllClusterInstanceHealthStatus(instances)).thenReturn(healthStatusDescMap);
        when(checkerService2.getAllClusterInstanceHealthStatus(instances)).thenReturn(healthStatusDescMap);

        Set<OuterClientService.HostPortDcStatus> hostPortDcStatuses = aggregatorPullService.getNeedAdjustInstances("cluster1", instances);
        Assert.assertEquals(1, hostPortDcStatuses.size());

        OuterClientService.HostPortDcStatus status = hostPortDcStatuses.iterator().next();
        Assert.assertEquals(hostPort2.getHost(), status.getHost());
        Assert.assertEquals(hostPort2.getPort(), status.getPort());
        Assert.assertEquals("jq", status.getDc());
        Assert.assertTrue(status.isCanRead());
    }
}
