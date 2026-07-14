package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.model.BeaconCheckConfigRequest;
import com.ctrip.xpipe.redis.console.service.BeaconCheckConfigService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ChangeConfigTest {

    @Mock
    private ConfigService configService;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private ClusterService clusterService;

    @Mock
    private BeaconCheckConfigService beaconCheckConfigService;

    @Mock
    private BeaconMetaService beaconMetaService;

    @InjectMocks
    private ChangeConfig controller;

    private String clusterName = "cluster1";

    private int noAlarmMinutesForClusterUpdate = 15;

    private int configDefaultRestoreHours = 1;

    @Before
    public void setupChangeConfigTest() {
        ClusterTbl clusterTbl = new ClusterTbl();
        clusterTbl.setClusterOrgId(1L);
        clusterTbl.setClusterType(ClusterType.SINGLE_DC.name());
        Mockito.when(clusterService.find(clusterName)).thenReturn(clusterTbl);
        Mockito.when(consoleConfig.supportSentinelBeacon(1L, clusterName)).thenReturn(true);
        Mockito.when(consoleConfig.getConfigDefaultRestoreHours()).thenReturn(configDefaultRestoreHours);
        Mockito.when(consoleConfig.getHealthCheckSuspendMinutes()).thenReturn(noAlarmMinutesForClusterUpdate);
    }

    @Test
    public void testStopSentinelCheck() throws Exception {
        AtomicInteger expectedMinutes = new AtomicInteger(0);
        Mockito.doAnswer(invocation -> {
            Integer minutes = invocation.getArgument(1, Integer.class);
            Assert.assertEquals(minutes.intValue(), expectedMinutes.get());
            return null;
        }).when(configService).stopSentinelCheck(Mockito.any(), Mockito.anyInt());

        expectedMinutes.set(noAlarmMinutesForClusterUpdate);
        controller.stopSentinelCheck(Mockito.mock(HttpServletRequest.class), clusterName, null);
        Mockito.verify(configService, Mockito.times(1)).stopSentinelCheck(Mockito.any(), Mockito.anyInt());

        expectedMinutes.set(30);
        controller.stopSentinelCheck(Mockito.mock(HttpServletRequest.class), clusterName, 30);
        Mockito.verify(configService, Mockito.times(2)).stopSentinelCheck(Mockito.any(), Mockito.anyInt());

        expectedMinutes.set(configDefaultRestoreHours * 60);
        controller.stopSentinelCheck(Mockito.mock(HttpServletRequest.class), clusterName, configDefaultRestoreHours * 60 + 10);
        Mockito.verify(configService, Mockito.times(3)).stopSentinelCheck(Mockito.any(), Mockito.anyInt());
    }

    @Test
    public void testStopAutoMigration() throws Exception {
        controller.setAllowAutoMigration(Mockito.mock(HttpServletRequest.class), false);
        Mockito.verify(configService, Mockito.times(1)).setAllowAutoMigration(false);

        controller.setAllowAutoMigration(Mockito.mock(HttpServletRequest.class), true);
        Mockito.verify(configService, Mockito.times(1)).setAllowAutoMigration(true);
    }

    @Test
    public void testStopBeaconCheck() throws Exception {
        AtomicInteger expectedMinutes = new AtomicInteger(0);
        BeaconCheckConfigRequest request = new BeaconCheckConfigRequest()
                .setClusterName(clusterName).setDc("jq").setShards(java.util.Collections.singletonList("shard1"));

        Mockito.doAnswer(invocation -> {
            Integer minutes = invocation.getArgument(3, Integer.class);
            Assert.assertEquals(minutes.intValue(), expectedMinutes.get());
            return null;
        }).when(beaconCheckConfigService).stopBeaconCheck(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyList(), Mockito.anyInt());

        expectedMinutes.set(noAlarmMinutesForClusterUpdate);
        controller.stopBeaconCheck(Mockito.mock(HttpServletRequest.class), null, request);
        Mockito.verify(beaconCheckConfigService, Mockito.times(1)).stopBeaconCheck(
                clusterName, "jq", request.getShards(), noAlarmMinutesForClusterUpdate);

        expectedMinutes.set(30);
        controller.stopBeaconCheck(Mockito.mock(HttpServletRequest.class), 30, request);
        Mockito.verify(beaconCheckConfigService, Mockito.times(1)).stopBeaconCheck(
                clusterName, "jq", request.getShards(), 30);

        expectedMinutes.set(configDefaultRestoreHours * 60);
        controller.stopBeaconCheck(Mockito.mock(HttpServletRequest.class), configDefaultRestoreHours * 60 + 10, request);
        Mockito.verify(beaconCheckConfigService, Mockito.times(1)).stopBeaconCheck(
                clusterName, "jq", request.getShards(), configDefaultRestoreHours * 60);
    }

    @Test
    public void testStartBeaconCheck() throws Exception {
        BeaconCheckConfigRequest request = new BeaconCheckConfigRequest()
                .setClusterName(clusterName).setDc("jq").setShards(java.util.Collections.singletonList("shard1"));
        controller.startBeaconCheck(Mockito.mock(HttpServletRequest.class), request);
        Mockito.verify(beaconCheckConfigService, Mockito.times(1)).startBeaconCheck(
                clusterName, "jq", request.getShards());
    }

    @Test
    public void testStopBeaconCheckRejectsEmptyShardInRequest() throws Exception {
        BeaconCheckConfigRequest request = new BeaconCheckConfigRequest()
                .setClusterName(clusterName).setDc("jq").setShards(java.util.Collections.singletonList(""));
        RetMessage result = controller.stopBeaconCheck(Mockito.mock(HttpServletRequest.class), 30, request);
        Assert.assertEquals(RetMessage.FAIL_STATE, result.getState());
        Assert.assertEquals("shard name can not be empty", result.getMessage());
    }

    @Test
    public void testStopBeaconCheckRejectsEmptyShards() throws Exception {
        BeaconCheckConfigRequest request = new BeaconCheckConfigRequest()
                .setClusterName(clusterName).setDc("jq").setShards(java.util.Collections.emptyList());
        RetMessage result = controller.stopBeaconCheck(Mockito.mock(HttpServletRequest.class), 30, request);
        Assert.assertEquals(RetMessage.FAIL_STATE, result.getState());
        Assert.assertEquals("shards can not be empty", result.getMessage());
    }

    @Test
    public void testStopBeaconCheckRejectsNonSentinelBeaconCluster() throws Exception {
        Mockito.when(consoleConfig.supportSentinelBeacon(1L, clusterName)).thenReturn(false);
        BeaconCheckConfigRequest request = new BeaconCheckConfigRequest()
                .setClusterName(clusterName).setDc("jq").setShards(java.util.Collections.singletonList("shard1"));
        RetMessage result = controller.stopBeaconCheck(Mockito.mock(HttpServletRequest.class), 30, request);
        Assert.assertEquals(RetMessage.FAIL_STATE, result.getState());
        Assert.assertEquals("cluster cluster1 is not managed by beacon sentinel mode", result.getMessage());
    }

}
