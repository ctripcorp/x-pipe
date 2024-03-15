package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfig;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_KEEPER_CONTAINER_STANDARD;
import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_SENTINEL_CHECK_EXCLUDE;

/**
 * @author chen.zhu
 * <p>
 * Nov 27, 2017
 */
public class ConfigServiceTest extends AbstractConsoleIntegrationTest {

    @Autowired
    ConfigService service;

    ConfigModel configModel;

    private String mockClusterName = "test-cluster";

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

    @Test
    public void testStartSentinelCheck() throws Exception {
        configModel.setSubKey(mockClusterName);
        service.startSentinelCheck(configModel);
        ConfigModel configModel = service.getConfig(KEY_SENTINEL_CHECK_EXCLUDE, mockClusterName);
        Assert.assertEquals(configModel.getVal(), String.valueOf(false));
    }

    @Test
    public void testGetConfigs() throws Exception {
        List<ConfigModel> configs = service.getConfigs(KEY_KEEPER_CONTAINER_STANDARD);
        Assert.assertEquals(configs.size(), 0);
    }

    @Test
    public void testStopSentinelCheck() throws Exception {
        configModel.setSubKey(mockClusterName);
        service.stopSentinelCheck(configModel, 1);
        ConfigModel configModel = service.getConfig(KEY_SENTINEL_CHECK_EXCLUDE, mockClusterName);
        Assert.assertEquals(configModel.getVal(), String.valueOf(true));
    }

    @Test
    public void testGetActiveSentinelCheckExcludeConfig() throws Exception  {
        configModel.setSubKey(mockClusterName);
        service.stopSentinelCheck(configModel, 0);
        sleep(1000);
        List<ConfigModel> configModels = service.getActiveSentinelCheckExcludeConfig();
        Assert.assertEquals(configModels.size(), 0);
        service.stopSentinelCheck(configModel, 1);
        configModels = service.getActiveSentinelCheckExcludeConfig();
        Assert.assertEquals(configModels.size(), 1);
        Assert.assertEquals(configModels.get(0).getSubKey(), mockClusterName);
        Assert.assertEquals(configModels.get(0).getVal(), String.valueOf(true));
    }

}