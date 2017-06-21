package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import org.junit.Assert;
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

    @Test
    public void test() throws DalException {

        String key = DefaultConsoleDbConfig.KEY_SENTINEL_AUTO_PROCESS;

        configDao.setKey(key, "true");

        Assert.assertTrue(consoleDbConfig.isSentinelAutoProcess());

        configDao.setKey(key, "false");

        Assert.assertFalse(consoleDbConfig.isSentinelAutoProcess());
    }


}
