package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_SENTINEL_AUTO_PROCESS;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
public class ConfigDaoTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ConfigDao configDao;

    private String key = KEY_SENTINEL_AUTO_PROCESS;

    @Test
    public void testCreateIfNotExist() throws DalException {

        String key = randomString(10);

        try{
            configDao.getKey(key);
            Assert.fail();
        }catch (DalNotFoundException e){

        }

        configDao.setKey(key, randomString(10));

        configDao.getKey(key);
    }

    @Test
    public void testGetSet() throws DalException, SQLException, IOException {

        logger.info("{}", configDao.findByKey(1));

        boolean boolValue = getBooleanValue(key);

        configDao.setKey(key, String.valueOf(!boolValue));

        boolean result = getBooleanValue(key);

        Assert.assertEquals(!boolValue, result);



    }


    @Test
    public void testSetKeyAndUntil() throws Exception {
        String localKey = "config.dao.test";
        String value = String.valueOf(false);
        Date date = DateTimeUtils.getHoursLaterDate(1);
        ConfigModel model = new ConfigModel().setKey(localKey).setVal(value);
        configDao.setConfigAndUntil(model, date);

        ConfigTbl config = configDao.getByKey(localKey);
        logger.info("config: {}", config);

        Assert.assertEquals(value, config.getValue());

        Assert.assertEquals(date, config.getUntil());
    }

    @Test
    public void testSetConfigAndUntil2() throws Exception {
        String localKey = "config.dao.test";
        String value = String.valueOf(true);
        Date date = DateTimeUtils.getHoursLaterDate(1);

        configDao.setKey(localKey, value);

        value = String.valueOf(false);
        ConfigModel model = new ConfigModel().setKey(localKey).setVal(value);
        configDao.setConfigAndUntil(model, date);

        ConfigTbl config = configDao.getByKey(localKey);
        logger.info("config: {}", config);

        Assert.assertEquals(value, config.getValue());

        Assert.assertEquals(date, config.getUntil());
    }

    private boolean getBooleanValue(String key) throws DalException {

        String strValue = configDao.getKey(key);
        logger.info("[getBooleanValue]{}, {}", key, strValue);
        return Boolean.parseBoolean(strValue);
    }



}
