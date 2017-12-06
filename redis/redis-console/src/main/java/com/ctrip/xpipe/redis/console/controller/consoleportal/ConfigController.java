package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @author chen.zhu
 * <p>
 * Dec 04, 2017
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class ConfigController extends AbstractConsoleController{

    private final Logger logger = LoggerFactory.getLogger(ConfigController.class);

    @Autowired
    private ConfigService configService;

    @RequestMapping(value = "/config/change_config", method = RequestMethod.POST)
    public RetMessage changeConfig(@RequestBody ConfigModel config) {
        logger.info("[changeConfig] Config Change To: {}", config);
        return changeConfig(config.getKey(), config.getVal());
    }

    @RequestMapping(value = "/config/alert_system", method = RequestMethod.GET)
    public RetMessage isAlertSystemOn() {
        if(configService.isAlertSystemOn()) {
            return RetMessage.createSuccessMessage();
        } else {
            return RetMessage.createFailMessage("closed");
        }
    }

    @RequestMapping(value = "/config/sentinel_auto_process", method = RequestMethod.GET)
    public RetMessage isSentinelAutoProcessOn() {
        if(configService.isSentinelAutoProcess()) {
            return RetMessage.createSuccessMessage();
        } else {
            return RetMessage.createFailMessage("closed");
        }
    }

    private RetMessage changeConfig(final String key, final String val) {
        try {
            boolean target = Boolean.parseBoolean(val);
            if(DefaultConsoleDbConfig.KEY_ALERT_SYSTEM_ON.equalsIgnoreCase(key)) {
                if(target) {
                    configService.startAlertSystem();
                } else {
                    configService.stopAlertSystem(DefaultConsoleDbConfig.SHUT_DOWN_HOURS);
                }
            } else if(DefaultConsoleDbConfig.KEY_SENTINEL_AUTO_PROCESS.equalsIgnoreCase(key)) {
                if(target) {
                    configService.startSentinelAutoProcess();
                } else {
                    configService.stopSentinelAutoProcess(DefaultConsoleDbConfig.SHUT_DOWN_HOURS);
                }
            } else {
                return RetMessage.createFailMessage("Unknown config key: " + key);
            }
            return RetMessage.createSuccessMessage();
        } catch (Exception e) {
            logger.error("[getExecuteFunction] Exception: {}", e);
            return RetMessage.createFailMessage(e.getMessage());
        }
    }
}
