package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.unidal.dal.jdbc.DalException;

import javax.servlet.http.HttpServletRequest;

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
    public void startSentinelAutoProcess(HttpServletRequest request,
                                         @RequestBody(required = false) ConfigModel configModel) throws DalException {
        ConfigModel config = configModel(request, configModel);
        configService.startSentinelAutoProcess(config);
    }

    @RequestMapping(value = "/config/sentinel_auto_process/stop", method = RequestMethod.POST)
    public void stopSentinelAutoProcess(HttpServletRequest request,
                                        @RequestBody(required = false) ConfigModel configModel) throws DalException {
        ConfigModel config = configModel(request, configModel);
        configService.stopSentinelAutoProcess(config, DefaultConsoleDbConfig.SHUT_DOWN_HOURS);
    }

    @RequestMapping(value = "/config/alert_system/start", method = RequestMethod.POST)
    public void startAlertSystem(HttpServletRequest request,
                                 @RequestBody(required = false) ConfigModel configModel) throws DalException {
        ConfigModel config = configModel(request, configModel);
        configService.startAlertSystem(config);
    }

    @RequestMapping(value = "/config/alert_system/stop", method = RequestMethod.POST)
    public void stopAlertSystem(HttpServletRequest request,
                                @RequestBody(required = false) ConfigModel configModel) throws DalException {
        ConfigModel config = configModel(request, configModel);
        configService.stopAlertSystem(config, DefaultConsoleDbConfig.SHUT_DOWN_HOURS);
    }

    @RequestMapping(value = "/config/alert_system/stop/{hours}", method = RequestMethod.POST)
    public void stopAlertSystem(HttpServletRequest request, @PathVariable int hours,
                                @RequestBody(required = false) ConfigModel configModel) throws DalException {
        ConfigModel config = configModel(request, configModel);
        hours = hours > DefaultConsoleDbConfig.SHUT_DOWN_HOURS || hours < 1 ? DefaultConsoleDbConfig.SHUT_DOWN_HOURS : hours;
        configService.stopAlertSystem(config, hours);
    }

    private ConfigModel configModel(HttpServletRequest request, ConfigModel configModel) {

        ConfigModel config = new ConfigModel().setUpdateIP(request.getRequestURI())
                .setUpdateUser(request.getRemoteUser());

        if(configModel != null && configModel.getUpdateUser() != null)
            config.setUpdateUser(configModel.getUpdateUser());

        return config;
    }
}
