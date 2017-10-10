package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.config.AbstractConfig;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.dal.jdbc.DalException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
@Component
public class DbConfig extends AbstractConfig{

    @Autowired
    private ConfigDao configDao;

    @Override
    public String get(String key) {
        try {
            return configDao.getKey(key);
        } catch (DalException e) {
            throw new IllegalStateException("find key error:" + key, e);
        }
    }
}
