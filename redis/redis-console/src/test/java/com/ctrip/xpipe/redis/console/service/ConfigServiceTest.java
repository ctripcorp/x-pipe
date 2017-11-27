package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Nov 27, 2017
 */
public class ConfigServiceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    ConfigService service;

    @Test
    public void startAlertSystem() throws Exception {
        service.startAlertSystem();
        Assert.assertTrue(service.isAlertSystemOn());
    }

    @Test
    public void startAlertSystem2() throws Exception {
        service.stopAlertSystem(1);
        service.startAlertSystem();
        Assert.assertTrue(service.isAlertSystemOn());
    }

    @Test
    public void stopAlertSystem() throws Exception {
        service.startAlertSystem();
        service.stopAlertSystem(3);
        Assert.assertFalse(service.isAlertSystemOn());
    }

    @Test
    public void startSentinelAutoProcess() throws Exception {
        service.startSentinelAutoProcess();
        Assert.assertTrue(service.isSentinelAutoProcess());
    }

    @Test
    public void stopSentinelAutoProcess() throws Exception {
        service.stopSentinelAutoProcess(2);
        Assert.assertFalse(service.isSentinelAutoProcess());
    }

    @Test
    public void isAlertSystemOn() throws Exception {
        service.stopAlertSystem(-2);
        Assert.assertTrue(service.isAlertSystemOn());
    }

    @Test
    public void isSentinelAutoProcess() throws Exception {
        service.stopSentinelAutoProcess(-7);
        Assert.assertTrue(service.isSentinelAutoProcess());
    }

}