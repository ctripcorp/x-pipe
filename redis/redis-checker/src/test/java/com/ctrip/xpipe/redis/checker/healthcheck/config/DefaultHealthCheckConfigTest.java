package com.ctrip.xpipe.redis.checker.healthcheck.config;


import com.ctrip.xpipe.redis.checker.RelationsService;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.delay.DelayConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultHealthCheckConfigTest {

    @InjectMocks
    private DefaultHealthCheckConfig config;

    @Mock
    private CheckerConfig checkerConfig;

    @Mock
    private RelationsService relationsService;

    @Test
    public void getDelayConfigTest() {
        when(checkerConfig.getHealthyDelayMilli()).thenReturn(2000);
        when(checkerConfig.getDownAfterCheckNums()).thenReturn(12);
        when(relationsService.getRegionDelay("dc1", "dc2")).thenReturn(null);
        when(relationsService.getDcsDelay("dc1", "dc2")).thenReturn(null);
        when(relationsService.getClusterDcsDelay("test", "dc1", "dc2")).thenReturn(null);

        DelayConfig delayConfig = config.getDelayConfig("test", "dc1", "dc2");
        Assert.assertEquals(2000, delayConfig.getDcLevelHealthyDelayMilli());
        Assert.assertEquals(2000, delayConfig.getClusterLevelHealthyDelayMilli());
        Assert.assertEquals(24000, delayConfig.getDcLevelDelayDownAfterMilli());
        Assert.assertEquals(24000, delayConfig.getClusterLevelDelayDownAfterMilli());

        when(relationsService.getDcsDelay("dc1", "dc2")).thenReturn(30000);
        delayConfig = config.getDelayConfig("test", "dc1", "dc2");
        Assert.assertEquals(30000, delayConfig.getDcLevelHealthyDelayMilli());
        Assert.assertEquals(30000, delayConfig.getClusterLevelHealthyDelayMilli());
        Assert.assertEquals(360000, delayConfig.getDcLevelDelayDownAfterMilli());
        Assert.assertEquals(360000, delayConfig.getClusterLevelDelayDownAfterMilli());

        when(relationsService.getClusterDcsDelay("test", "dc1", "dc2")).thenReturn(-2000);
        delayConfig = config.getDelayConfig("test", "dc1", "dc2");
        Assert.assertEquals(30000, delayConfig.getDcLevelHealthyDelayMilli());
        Assert.assertEquals(-2000, delayConfig.getClusterLevelHealthyDelayMilli());
        Assert.assertEquals(360000, delayConfig.getDcLevelDelayDownAfterMilli());
        Assert.assertEquals(-24000, delayConfig.getClusterLevelDelayDownAfterMilli());
    }

    @Test
    public void getDelayConfigRegionLayerTest() {
        when(checkerConfig.getHealthyDelayMilli()).thenReturn(2000);
        when(checkerConfig.getDownAfterCheckNums()).thenReturn(12);
        when(relationsService.getRegionDelay("dc1", "dc2")).thenReturn(3000);
        when(relationsService.getDcsDelay("dc1", "dc2")).thenReturn(null);
        when(relationsService.getClusterDcsDelay("test", "dc1", "dc2")).thenReturn(null);

        DelayConfig delayConfig = config.getDelayConfig("test", "dc1", "dc2");
        Assert.assertEquals(3000, delayConfig.getDcLevelHealthyDelayMilli());
        Assert.assertEquals(3000, delayConfig.getClusterLevelHealthyDelayMilli());
        Assert.assertEquals(36000, delayConfig.getDcLevelDelayDownAfterMilli());
        Assert.assertEquals(36000, delayConfig.getClusterLevelDelayDownAfterMilli());

        when(relationsService.getDcsDelay("dc1", "dc2")).thenReturn(5000);
        delayConfig = config.getDelayConfig("test", "dc1", "dc2");
        Assert.assertEquals(5000, delayConfig.getDcLevelHealthyDelayMilli());
        Assert.assertEquals(5000, delayConfig.getClusterLevelHealthyDelayMilli());

        when(relationsService.getClusterDcsDelay("test", "dc1", "dc2")).thenReturn(7000);
        delayConfig = config.getDelayConfig("test", "dc1", "dc2");
        Assert.assertEquals(5000, delayConfig.getDcLevelHealthyDelayMilli());
        Assert.assertEquals(7000, delayConfig.getClusterLevelHealthyDelayMilli());
    }

}
