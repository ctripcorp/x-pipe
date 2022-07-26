package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.OuterClientCache;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DefaultDelayPingActionCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HEALTH_STATE;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatus;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStatusDesc;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.OutClientInstanceHealthHolder;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.UpDownInstances;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

/**
 * @author lishanglin
 * date 2022/7/24
 */
@RunWith(MockitoJUnitRunner.class)
public class InstanceHealthStatusCollectorTest extends AbstractTest {

    @Mock
    private DefaultDelayPingActionCollector delayPingActionCollector;

    @Mock
    private RemoteCheckerManager remoteCheckerManager;

    @Mock
    private OuterClientCache outerClientCache;

    @Mock
    private CheckerService checkerService;

    private InstanceHealthStatusCollector collector;

    private Map<String, Set<HostPort>> interested;

    @Before
    public void setupInstanceHealthStatusCollectorTest() {
        this.collector = new InstanceHealthStatusCollector(delayPingActionCollector, remoteCheckerManager,
                outerClientCache, executors);
        Mockito.when(remoteCheckerManager.getAllCheckerServices()).thenReturn(Collections.singletonList(checkerService));
        interested = new HashMap<>();
        interested.put("cluster1", new HashSet<>(Arrays.asList(new HostPort("10.0.0.1", 6379))));
    }

    @Test
    public void testCollect() throws Exception {
        Map<HostPort, HealthStatusDesc> healthStatus = mockHealthStatusMap(HEALTH_STATE.HEALTHY);
        Mockito.when(checkerService.getAllInstanceHealthStatus()).thenReturn(healthStatus);
        Mockito.when(outerClientCache.getAllActiveDcClusters("jq")).thenReturn(mockOutClientResp(true));
        healthStatus = mockHealthStatusMap(HEALTH_STATE.HEALTHY);
        Mockito.when(delayPingActionCollector.getAllHealthStatus()).thenReturn(healthStatus);
        Pair<XPipeInstanceHealthHolder, OutClientInstanceHealthHolder> result = this.collector.collect();

        UpDownInstances xpipeUpDownInstances = result.getKey().aggregate(interested, 2);
        Assert.assertEquals(Collections.singleton(new HostPort("10.0.0.1", 6379)), xpipeUpDownInstances.getHealthyInstances());
        Assert.assertTrue(xpipeUpDownInstances.getUnhealthyInstances().isEmpty());

        UpDownInstances outClientUpDownInstances = result.getValue().extractReadable(interested);
        Assert.assertEquals(Collections.singleton(new HostPort("10.0.0.1", 6379)), outClientUpDownInstances.getHealthyInstances());
        Assert.assertTrue(outClientUpDownInstances.getUnhealthyInstances().isEmpty());
    }

    @Test
    public void testXPipeAggregateQuorum() {
        XPipeInstanceHealthHolder holder = new XPipeInstanceHealthHolder();
        holder.add(mockHealthStatusMap(HEALTH_STATE.INSTANCEUP));
        holder.add(mockHealthStatusMap(HEALTH_STATE.HEALTHY));
        holder.add(mockHealthStatusMap(HEALTH_STATE.DOWN));
        holder.add(mockHealthStatusMap(HEALTH_STATE.UNHEALTHY));

        UpDownInstances xpipeUpDownInstances = holder.aggregate(interested, 2);
        Assert.assertTrue(xpipeUpDownInstances.getHealthyInstances().isEmpty());
        Assert.assertTrue(xpipeUpDownInstances.getUnhealthyInstances().isEmpty());
    }

    @Test
    public void testXPipeAggregateDown() {
        XPipeInstanceHealthHolder holder = new XPipeInstanceHealthHolder();
        holder.add(mockHealthStatusMap(HEALTH_STATE.UNHEALTHY));
        holder.add(mockHealthStatusMap(HEALTH_STATE.DOWN));
        holder.add(mockHealthStatusMap(HEALTH_STATE.SICK));

        UpDownInstances xpipeUpDownInstances = holder.aggregate(interested, 2);
        Assert.assertTrue(xpipeUpDownInstances.getHealthyInstances().isEmpty());
        Assert.assertEquals(Collections.singleton(new HostPort("10.0.0.1", 6379)), xpipeUpDownInstances.getUnhealthyInstances());
    }

    @Test
    public void testOutClientStatusDown() {
        OutClientInstanceHealthHolder outClientInstanceHealthHolder = new OutClientInstanceHealthHolder();
        outClientInstanceHealthHolder.addClusters(mockOutClientResp(false));

        UpDownInstances outClientUpDownInstances = outClientInstanceHealthHolder.extractReadable(interested);
        Assert.assertTrue(outClientUpDownInstances.getHealthyInstances().isEmpty());
        Assert.assertEquals(Collections.singleton(new HostPort("10.0.0.1", 6379)), outClientUpDownInstances.getUnhealthyInstances());
    }

    private Map<HostPort, HealthStatusDesc> mockHealthStatusMap(HEALTH_STATE healthState) {
        HealthStatus healthStatus = Mockito.mock(HealthStatus.class);
        Mockito.when(healthStatus.getState()).thenReturn(healthState);
        Mockito.when(healthStatus.getLastPongTime()).thenReturn(-1L);
        Mockito.when(healthStatus.getLastHealthyDelayTime()).thenReturn(-1L);
        HostPort instance = new HostPort("10.0.0.1", 6379);
        return Collections.singletonMap(instance, new HealthStatusDesc(instance, healthStatus));
    }

    private Map<String, OuterClientService.ClusterInfo> mockOutClientResp(boolean canRead) {
        OuterClientService.ClusterInfo clusterInfo = new OuterClientService.ClusterInfo();
        OuterClientService.GroupInfo groupInfo = new OuterClientService.GroupInfo();
        OuterClientService.InstanceInfo instanceInfo = new OuterClientService.InstanceInfo();
        instanceInfo.setIPAddress("10.0.0.1").setPort(6379).setCanRead(canRead);
        clusterInfo.setGroups(Collections.singletonList(groupInfo.setInstances(Collections.singletonList(instanceInfo))));
        return Collections.singletonMap("cluster1", clusterInfo);
    }

}
