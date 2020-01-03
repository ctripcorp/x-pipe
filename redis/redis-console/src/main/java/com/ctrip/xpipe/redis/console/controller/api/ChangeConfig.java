package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.ClusterConfigModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.unidal.dal.jdbc.DalException;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.stream.Collectors;

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

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private ClusterService clusterService;

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
        configService.stopSentinelAutoProcess(config, consoleConfig.getConfigDefaultRestoreHours());
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
        configService.stopAlertSystem(config, consoleConfig.getConfigDefaultRestoreHours());
    }

    @RequestMapping(value = "/config/alert_system/stop/{hours}", method = RequestMethod.POST)
    public void stopAlertSystem(HttpServletRequest request, @PathVariable int hours,
                                @RequestBody(required = false) ConfigModel configModel) throws DalException {
        ConfigModel config = configModel(request, configModel);
        int defaultHours = consoleConfig.getConfigDefaultRestoreHours();
        hours = hours > defaultHours || hours < 1 ? defaultHours : hours;
        configService.stopAlertSystem(config, hours);
    }

    @RequestMapping(value = "/config/ignore/migration/system/availability/{ignore}", method = RequestMethod.POST)
    public void setIgnoreMigrationSystemAvailOrNot(HttpServletRequest request, @PathVariable boolean ignore) throws DalException {
        String sourceIp = request.getHeader("X-FORWARDED-FOR");
        if(sourceIp == null) {
            sourceIp = request.getRemoteAddr();
        }
        logger.info("[setIgnoreMigrationSystemAvailOrNot][{}] ignore: {}", sourceIp, ignore);
        configService.doIgnoreMigrationSystemAvailability(ignore);
    }

    @RequestMapping(value = "/config/sentinel/check/exclude", method = RequestMethod.POST)
    public RetMessage setSentinelCheckExcludeConfig(HttpServletRequest request,
                                                    @RequestBody ClusterConfigModel configModel) throws DalException {
        if (StringUtil.isEmpty(configModel.getClusterName())) throw new IllegalArgumentException("cluster can not be empty");
        ClusterTbl clusterTbl = clusterService.find(configModel.getClusterName());
        if (null == clusterTbl) throw new IllegalArgumentException("cluster not exist");

        ConfigModel config = configModel(request, null);
        config.setSubKey(configModel.getClusterName());

        if (Boolean.TRUE.equals(configModel.getValue())) {
            configService.stopSentinelCheck(config, consoleConfig.getNoAlarmMinutesForClusterUpdate());
        } else {
            configService.startSentinelCheck(config);
        }

        return RetMessage.createSuccessMessage("success");
    }

    @RequestMapping(value = "/config/sentinel/check/exclude", method = RequestMethod.GET)
    public ClusterConfigModel getSentinelCheckExcludeConfig(@RequestParam String clusterName) {
        ConfigModel model = configService.getConfig(DefaultConsoleDbConfig.KEY_SENTINEL_CHECK_EXCLUDE, clusterName);
        return new ClusterConfigModel(model.getSubKey(), Boolean.valueOf(model.getVal()));
    }

    @RequestMapping(value = "/config/sentinel/check/exclude/all", method = RequestMethod.GET)
    public List<String> getAllSentinelCheckExcludeConfig() {
        List<ConfigModel> configModels = configService.getActiveSentinelCheckExcludeConfig();
        return configModels.stream().map(ConfigModel::getSubKey).collect(Collectors.toList());
    }

    private ConfigModel configModel(HttpServletRequest request, ConfigModel configModel) {

        String sourceIp = request.getHeader("X-FORWARDED-FOR");
        if(sourceIp == null) {
            sourceIp = request.getRemoteAddr();
        }
        ConfigModel config = new ConfigModel().setUpdateIP(sourceIp)
                .setUpdateUser(request.getRemoteUser());

        if(configModel != null && configModel.getUpdateUser() != null)
            config.setUpdateUser(configModel.getUpdateUser());

        return config;
    }
}
