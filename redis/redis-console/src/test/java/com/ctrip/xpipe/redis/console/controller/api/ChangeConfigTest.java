package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
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

    @InjectMocks
    private ChangeConfig controller;

    private String clusterName = "cluster1";

    private int noAlarmMinutesForClusterUpdate = 15;

    private int configDefaultRestoreHours = 1;

    @Before
    public void setupChangeConfigTest() {
        Mockito.when(clusterService.find(clusterName)).thenReturn(new ClusterTbl());
        Mockito.when(consoleConfig.getConfigDefaultRestoreHours()).thenReturn(configDefaultRestoreHours);
        Mockito.when(consoleConfig.getNoAlarmMinutesForClusterUpdate()).thenReturn(noAlarmMinutesForClusterUpdate);
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


}
