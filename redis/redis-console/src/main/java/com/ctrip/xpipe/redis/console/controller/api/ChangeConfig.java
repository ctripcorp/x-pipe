package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
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
    private ConfigService configService;

    @RequestMapping(value = "/config/sentinel_auto_process/start", method = RequestMethod.POST)
    public void startSentinelAutoProcess() throws DalException {

        configService.startSentinelAutoProcess();
    }

    @RequestMapping(value = "/config/sentinel_auto_process/stop", method = RequestMethod.POST)
    public void stopSentinelAutoProcess() throws DalException {
        int hours = 8;
        configService.stopSentinelAutoProcess(hours);
    }

    @RequestMapping(value = "/config/alert_system/start", method = RequestMethod.POST)
    public void startAlertSystem() throws DalException {

        configService.startAlertSystem();
    }

    @RequestMapping(value = "/config/alert_system/stop", method = RequestMethod.POST)
    public void stopAlertSystem() throws DalException {
        int hours = 8;
        configService.stopAlertSystem(hours);
    }

    @RequestMapping(value = "/config/alert_system/stop/{hours}", method = RequestMethod.POST)
    public void stopAlertSystem(@PathVariable int hours) throws DalException {

        hours = hours > 8 || hours < 1 ? 8 : hours;
        configService.stopAlertSystem(hours);
    }
}
