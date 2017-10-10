package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.unidal.dal.jdbc.DalException;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX)
public class ChangeConfig extends AbstractConsoleController{

    @Autowired
    private ConfigDao configDao;

    @RequestMapping(value = "/config/sentinel_auto_process/start", method = RequestMethod.POST)
    public void startSentinelAutoProcess() throws DalException {

        configDao.setKey(DefaultConsoleDbConfig.KEY_SENTINEL_AUTO_PROCESS, String.valueOf(true));
    }

    @RequestMapping(value = "/config/sentinel_auto_process/stop", method = RequestMethod.POST)
    public void stopSentinelAutoProcess() throws DalException {

        configDao.setKey(DefaultConsoleDbConfig.KEY_SENTINEL_AUTO_PROCESS, String.valueOf(false));
    }


}
