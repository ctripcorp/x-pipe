package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.CheckerService;
import com.ctrip.xpipe.redis.checker.RemoteCheckerManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.compensator.data.XPipeInstanceHealthHolder;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

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

    @Mock
    private DefaultDelayPingActionCollector defaultDelayPingActionCollector;
    @Mock
    private DefaultPsubPingActionCollector defaultPsubPingActionCollector;

    private static final String LOCAL_IP = "127.0.0.1";
    private HostPort hostPort1 = new HostPort(LOCAL_IP, 6379);
    private HostPort hostPort2 = new HostPort(LOCAL_IP, 6380);
    private HostPort hostPort3 = new HostPort(LOCAL_IP, 6381);

    private static final String JQ = "jq";
    private static final String OY = "oy";
    private static final String AWS = "aws";

    @Before
    public void setupDefaultAggregatorPullServiceTest() throws Exception {
        aggregatorPullService.setExecutors(MoreExecutors.directExecutor());

        when(remoteCheckerManager.getAllCheckerServices()).thenReturn(Arrays.asList(checkerService1, checkerService2));
        when(config.getQuorum()).thenReturn(2);

        Map<HostPort, OuterClientService.OutClientInstanceStatus> outerClientState = new HashMap<>();
        outerClientState.put(hostPort1, new OuterClientService.OutClientInstanceStatus().setEnv(JQ).setIPAddress(LOCAL_IP).setPort(hostPort1.getPort()).setCanRead(true));
        outerClientState.put(hostPort2, new OuterClientService.OutClientInstanceStatus().setEnv(JQ).setIPAddress(LOCAL_IP).setPort(hostPort2.getPort()).setCanRead(false));
        outerClientState.put(hostPort3, new OuterClientService.OutClientInstanceStatus().setEnv(AWS).setIPAddress(LOCAL_IP).setPort(hostPort3.getPort()).setCanRead(true).setSuspect(true));
        when(outerClientService.batchQueryInstanceStatus(anyString(), anySet())).thenReturn(outerClientState);
        when(metaCache.getDc(hostPort1)).thenReturn(JQ);
        when(metaCache.getDc(hostPort2)).thenReturn(JQ);
        when(metaCache.getDc(hostPort3)).thenReturn(AWS);

        when(metaCache.isCrossRegion(JQ, AWS)).thenReturn(true);
        when(metaCache.isCrossRegion(JQ, OY)).thenReturn(false);
        when(metaCache.isCrossRegion(JQ, JQ)).thenReturn(false);
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
        Set<HostPort> instances = Sets.newHashSet(hostPort1, hostPort2, hostPort3);
        Map<HostPort, HealthStatusDesc> healthStatusDescMap = new HashMap<>();
        healthStatusDescMap.put(hostPort1, new HealthStatusDesc(hostPort1, HEALTH_STATE.DOWN));
        healthStatusDescMap.put(hostPort2, new HealthStatusDesc(hostPort1, HEALTH_STATE.DOWN));
        healthStatusDescMap.put(hostPort3, new HealthStatusDesc(hostPort3, HEALTH_STATE.DOWN));
        when(checkerService1.getAllClusterInstanceHealthStatus(instances)).thenReturn(healthStatusDescMap);
        when(checkerService2.getAllClusterInstanceHealthStatus(instances)).thenReturn(healthStatusDescMap);

        Set<OuterClientService.HostPortDcStatus> hostPortDcStatuses = aggregatorPullService.getNeedAdjustInstances("cluster1", instances);
        Assert.assertEquals(1, hostPortDcStatuses.size());

        OuterClientService.HostPortDcStatus status = hostPortDcStatuses.iterator().next();
        Assert.assertEquals(hostPort1.getHost(), status.getHost());
        Assert.assertEquals(hostPort1.getPort(), status.getPort());
        Assert.assertEquals("jq", status.getDc());
        Assert.assertFalse(status.isCanRead());

        healthStatusDescMap.put(hostPort3, new HealthStatusDesc(hostPort3, HEALTH_STATE.HEALTHY));
        when(checkerService1.getAllClusterInstanceHealthStatus(instances)).thenReturn(healthStatusDescMap);
        when(checkerService2.getAllClusterInstanceHealthStatus(instances)).thenReturn(healthStatusDescMap);

        hostPortDcStatuses = aggregatorPullService.getNeedAdjustInstances("cluster1", instances);
        Assert.assertEquals(2, hostPortDcStatuses.size());

        hostPortDcStatuses.forEach(hostPortDcStatus -> {
            if (hostPortDcStatus.getPort() == hostPort1.getPort()) {
                Assert.assertEquals(hostPort1.getHost(), hostPortDcStatus.getHost());
                Assert.assertEquals(JQ, hostPortDcStatus.getDc());
                Assert.assertFalse(hostPortDcStatus.isCanRead());
            } else {
                Assert.assertEquals(hostPort3.getHost(), hostPortDcStatus.getHost());
                Assert.assertEquals(hostPort3.getPort(), hostPortDcStatus.getPort());
                Assert.assertEquals(AWS, hostPortDcStatus.getDc());
                Assert.assertTrue(hostPortDcStatus.isCanRead());
            }
        });
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

    @Test
    public void doMarkInstances() throws Exception {
        Set<OuterClientService.HostPortDcStatus> hostPortDcStatuses = Sets.newHashSet(
                new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6379, OY, true),
                new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6380, OY, false),
                new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6381, AWS, true),
                new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6382, AWS, false)
        );
        aggregatorPullService.doMarkInstances("cluster1", JQ, hostPortDcStatuses);

        for (OuterClientService.HostPortDcStatus status : hostPortDcStatuses) {
            if (status.getPort() == 6379) {
                Assert.assertEquals(LOCAL_HOST, status.getHost());
                Assert.assertEquals(OY, status.getDc());
                Assert.assertTrue(status.isCanRead());
                Assert.assertFalse(status.isSuspect());
            } else if (status.getPort() == 6380) {
                Assert.assertEquals(LOCAL_HOST, status.getHost());
                Assert.assertEquals(OY, status.getDc());
                Assert.assertFalse(status.isCanRead());
                Assert.assertFalse(status.isSuspect());
            } else if (status.getPort() == 6381) {
                Assert.assertEquals(LOCAL_HOST, status.getHost());
                Assert.assertEquals(AWS, status.getDc());
                Assert.assertTrue(status.isCanRead());
                Assert.assertFalse(status.isSuspect());
            } else {
                Assert.assertEquals(6382, status.getPort());
                Assert.assertEquals(LOCAL_HOST, status.getHost());
                Assert.assertEquals(AWS, status.getDc());
                Assert.assertFalse(status.isCanRead());
                Assert.assertTrue(status.isSuspect());
            }
        }

        verify(outerClientService, times(1)).batchMarkInstance(ArgumentMatchers.argThat(argument -> argument.getNoModifySeconds() == null));
    }

    @Test
    public void doMarkInstancesIfNoModifyFor() throws Exception {
        Set<OuterClientService.HostPortDcStatus> hostPortDcStatuses = Sets.newHashSet(
                new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6379, OY, true),
                new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6380, OY, false),
                new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6381, AWS, true),
                new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6382, AWS, false)
        );
        aggregatorPullService.doMarkInstancesIfNoModifyFor("cluster1", JQ, hostPortDcStatuses,1);

        for (OuterClientService.HostPortDcStatus status : hostPortDcStatuses) {
            if (status.getPort() == 6379) {
                Assert.assertEquals(LOCAL_HOST, status.getHost());
                Assert.assertEquals(OY, status.getDc());
                Assert.assertTrue(status.isCanRead());
                Assert.assertFalse(status.isSuspect());
            } else if (status.getPort() == 6380) {
                Assert.assertEquals(LOCAL_HOST, status.getHost());
                Assert.assertEquals(OY, status.getDc());
                Assert.assertFalse(status.isCanRead());
                Assert.assertFalse(status.isSuspect());
            } else if (status.getPort() == 6381) {
                Assert.assertEquals(LOCAL_HOST, status.getHost());
                Assert.assertEquals(AWS, status.getDc());
                Assert.assertTrue(status.isCanRead());
                Assert.assertFalse(status.isSuspect());
            } else {
                Assert.assertEquals(6382, status.getPort());
                Assert.assertEquals(LOCAL_HOST, status.getHost());
                Assert.assertEquals(AWS, status.getDc());
                Assert.assertFalse(status.isCanRead());
                Assert.assertTrue(status.isSuspect());
            }
        }

        verify(outerClientService, times(1)).batchMarkInstance(ArgumentMatchers.argThat(argument -> argument.getNoModifySeconds() == 1));
    }

    @Test
    public void dcInstancesAllUpTest() throws Exception {
        Map<String, List<RedisMeta>> dcInstances = new HashMap<>();
        dcInstances.put(JQ, Lists.newArrayList(new RedisMeta().setIp(LOCAL_IP).setPort(6379), new RedisMeta().setIp(LOCAL_IP).setPort(6380)));
        dcInstances.put(OY, Lists.newArrayList(new RedisMeta().setIp(LOCAL_IP).setPort(6381), new RedisMeta().setIp(LOCAL_IP).setPort(6382)));
        dcInstances.put(AWS, Lists.newArrayList(new RedisMeta().setIp(LOCAL_IP).setPort(6383), new RedisMeta().setIp(LOCAL_IP).setPort(6384)));
        when(metaCache.getAllInstance("test")).thenReturn(dcInstances);

        when(metaCache.getDc(new HostPort(LOCAL_HOST, 6379))).thenReturn(JQ);
        when(metaCache.getDc(new HostPort(LOCAL_HOST, 6380))).thenReturn(JQ);
        when(metaCache.getDc(new HostPort(LOCAL_HOST, 6381))).thenReturn(OY);
        when(metaCache.getDc(new HostPort(LOCAL_HOST, 6382))).thenReturn(OY);
        when(metaCache.getDc(new HostPort(LOCAL_HOST, 6383))).thenReturn(AWS);
        when(metaCache.getDc(new HostPort(LOCAL_HOST, 6384))).thenReturn(AWS);

        Map<HostPort, HealthStatusDesc> allStatus = new HashMap<>();
        allStatus.put(new HostPort(LOCAL_IP, 6379), new HealthStatusDesc(new HostPort(LOCAL_IP, 6379), HEALTH_STATE.HEALTHY));
        allStatus.put(new HostPort(LOCAL_IP, 6380), new HealthStatusDesc(new HostPort(LOCAL_IP, 6380), HEALTH_STATE.HEALTHY));
        allStatus.put(new HostPort(LOCAL_IP, 6381), new HealthStatusDesc(new HostPort(LOCAL_IP, 6381), HEALTH_STATE.DOWN));
        allStatus.put(new HostPort(LOCAL_IP, 6382), new HealthStatusDesc(new HostPort(LOCAL_IP, 6382), HEALTH_STATE.SICK));
        allStatus.put(new HostPort(LOCAL_IP, 6383), new HealthStatusDesc(new HostPort(LOCAL_IP, 6383), HEALTH_STATE.HEALTHY));
        allStatus.put(new HostPort(LOCAL_IP, 6384), new HealthStatusDesc(new HostPort(LOCAL_IP, 6384), HEALTH_STATE.HEALTHY));

        when(defaultDelayPingActionCollector.getAllHealthStatus()).thenReturn(allStatus);
        Assert.assertNull(aggregatorPullService.dcInstancesAllUp("test", JQ, Sets.newHashSet(new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6381, OY, true))));
        Assert.assertNotNull(aggregatorPullService.dcInstancesAllUp("test", JQ, Sets.newHashSet(new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6383, AWS, true))));
        Assert.assertEquals(AWS, aggregatorPullService.dcInstancesAllUp("test", JQ, Sets.newHashSet(new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6381, OY, true), new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6383, AWS, true))));
        verify(defaultPsubPingActionCollector, never()).getAllHealthStatus();
        verify(defaultDelayPingActionCollector, times(4)).getAllHealthStatus();


        Map<HostPort, HealthStatusDesc> allStatusCrossRegion = new HashMap<>();
        allStatusCrossRegion.put(new HostPort(LOCAL_IP, 6383), new HealthStatusDesc(new HostPort(LOCAL_IP, 6383), HEALTH_STATE.DOWN));
        allStatusCrossRegion.put(new HostPort(LOCAL_IP, 6384), new HealthStatusDesc(new HostPort(LOCAL_IP, 6384), HEALTH_STATE.DOWN));
        when(defaultPsubPingActionCollector.getAllHealthStatus()).thenReturn(allStatusCrossRegion);
        Assert.assertNull(aggregatorPullService.dcInstancesAllUp("test", AWS, Sets.newHashSet(new OuterClientService.HostPortDcStatus(LOCAL_HOST, 6383, AWS, true))));
        verify(defaultPsubPingActionCollector, times(1)).getAllHealthStatus();
        verify(defaultDelayPingActionCollector, times(4)).getAllHealthStatus();
    }
}
