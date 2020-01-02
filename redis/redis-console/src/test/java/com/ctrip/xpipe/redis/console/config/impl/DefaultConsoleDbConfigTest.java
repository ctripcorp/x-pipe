package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
public class DefaultConsoleDbConfigTest extends AbstractConsoleIntegrationTest{

    @Autowired
    private ConsoleDbConfig consoleDbConfig;

    @Autowired
    private ConfigDao configDao;

    @Autowired
    private ConfigService service;

    private ConfigModel configModel;

    @Before
    public void beforeDefaultConsoleDbConfigTest() {
        configModel = new ConfigModel().setUpdateIP("localhost").setUpdateUser("System");
    }

    @Test
    public void test() throws DalException {

        String key = DefaultConsoleDbConfig.KEY_SENTINEL_AUTO_PROCESS;

        configModel.setKey(key).setVal("true");
        configDao.setConfig(configModel);

        Assert.assertTrue(consoleDbConfig.isSentinelAutoProcess());

        service.stopSentinelAutoProcess(configModel, 3);

        Assert.assertFalse(consoleDbConfig.isSentinelAutoProcess());

        service.stopSentinelAutoProcess(configModel, -1);
        Assert.assertTrue(consoleDbConfig.isSentinelAutoProcess());

        service.stopSentinelAutoProcess(configModel, 8);
        Assert.assertFalse(consoleDbConfig.isSentinelAutoProcess());

        service.startSentinelAutoProcess(configModel);
        Assert.assertTrue(consoleDbConfig.isSentinelAutoProcess());
    }

    @Test
    public void testShouldSentinelCheck() throws DalException {
        String key = DefaultConsoleDbConfig.KEY_SENTINEL_CHECK_EXCLUDE;
        String mockCluster = "test-cluster";
        configModel.setKey(key).setSubKey(mockCluster);
        configModel.setVal("true");

        service.stopSentinelCheck(configModel, 0);
        sleep(1000);
        Assert.assertTrue(consoleDbConfig.shouldSentinelCheck(mockCluster, true));

        service.stopSentinelCheck(configModel, 1);
        Assert.assertFalse(consoleDbConfig.shouldSentinelCheck(mockCluster, true));

        service.startSentinelCheck(configModel);
        Assert.assertTrue(consoleDbConfig.shouldSentinelCheck(mockCluster, true));
    }

    @Test
    public void testShouldSentinelCheckWithCache() throws DalException {
        String key = DefaultConsoleDbConfig.KEY_SENTINEL_CHECK_EXCLUDE;
        String mockCluster = "test-cluster";
        configModel.setKey(key).setSubKey(mockCluster);
        configModel.setVal("true");

        service.stopSentinelCheck(configModel, 1);
        Assert.assertFalse(consoleDbConfig.shouldSentinelCheck(mockCluster, false));

        service.startSentinelCheck(configModel);
        Assert.assertFalse(consoleDbConfig.shouldSentinelCheck(mockCluster, false));
    }

    @Test
    public void testSentinelCheckWhiteList() throws DalException {
        String key = DefaultConsoleDbConfig.KEY_SENTINEL_CHECK_EXCLUDE;
        String mockCluster1 = "test-cluster1";
        String mockCluster2 = "test-cluster2";
        String mockCluster3 = "test-cluster3";

        configModel.setKey(key);
        configModel.setSubKey(mockCluster1);
        service.stopSentinelCheck(configModel, 1);

        configModel.setSubKey(mockCluster2);
        service.stopSentinelCheck(configModel, 5);

        configModel.setSubKey(mockCluster3);
        service.startSentinelCheck(configModel);

        Set<String> whitelist = consoleDbConfig.sentinelCheckWhiteList(true);
        Assert.assertTrue(whitelist.contains(mockCluster1));
        Assert.assertTrue(whitelist.contains(mockCluster2));
        Assert.assertFalse(whitelist.contains(mockCluster3));
    }


}
