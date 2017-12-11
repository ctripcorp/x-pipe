package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Nov 27, 2017
 */
public class ConfigServiceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    ConfigService service;

    ConfigModel configModel;

    @Before
    public void beforeConfigServiceTest() {
        configModel = new ConfigModel().setUpdateUser("System").setUpdateIP("localhost");
    }

    @Test
    public void startAlertSystem() throws Exception {
        service.startAlertSystem(configModel);
        Assert.assertTrue(service.isAlertSystemOn());
    }

    @Test
    public void startAlertSystem2() throws Exception {
        service.stopAlertSystem(configModel, 1);
        service.startAlertSystem(configModel);
        Assert.assertTrue(service.isAlertSystemOn());
    }

    @Test
    public void stopAlertSystem() throws Exception {
        service.startAlertSystem(configModel);
        service.stopAlertSystem(configModel, 3);
        Assert.assertFalse(service.isAlertSystemOn());
    }

    @Test
    public void startSentinelAutoProcess() throws Exception {
        service.startSentinelAutoProcess(configModel);
        Assert.assertTrue(service.isSentinelAutoProcess());
    }

    @Test
    public void stopSentinelAutoProcess() throws Exception {
        service.stopSentinelAutoProcess(configModel, 2);
        Assert.assertFalse(service.isSentinelAutoProcess());
    }

    @Test
    public void isAlertSystemOn() throws Exception {
        service.stopAlertSystem(configModel, -2);
        Assert.assertTrue(service.isAlertSystemOn());
    }

    @Test
    public void isSentinelAutoProcess() throws Exception {
        service.stopSentinelAutoProcess(configModel, -7);
        Assert.assertTrue(service.isSentinelAutoProcess());
    }

}