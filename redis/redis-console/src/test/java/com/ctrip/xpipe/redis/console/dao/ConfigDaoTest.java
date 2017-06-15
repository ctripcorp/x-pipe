package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
public class ConfigDaoTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ConfigDao configDao;

    private String key = "sentinel.auto.process";

    @Test
    public void testGetSet() throws DalException, SQLException, IOException {

        startH2Server();

        logger.info("{}", configDao.findByKey(1));

        boolean boolValue = getBooleanValue(key);

        configDao.setKey(key, String.valueOf(!boolValue));

        boolean result = getBooleanValue(key);

        Assert.assertEquals(!boolValue, result);



    }

    private boolean getBooleanValue(String key) throws DalException {

        String strValue = configDao.getKey(key);
        logger.info("[getBooleanValue]{}, {}", key, strValue);
        return Boolean.parseBoolean(strValue);
    }

}
