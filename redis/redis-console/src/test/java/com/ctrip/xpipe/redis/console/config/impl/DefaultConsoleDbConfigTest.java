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


}
