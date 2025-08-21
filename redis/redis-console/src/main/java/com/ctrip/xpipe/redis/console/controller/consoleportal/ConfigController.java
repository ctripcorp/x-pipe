package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.api.sso.UserInfo;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_ALERT_SYSTEM_ON;
import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_SENTINEL_AUTO_PROCESS;

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

    @Autowired
    private ConsoleConfig consoleConfig;

    @RequestMapping(value = "/config/change_config", method = RequestMethod.POST)
    public RetMessage changeConfig(HttpServletRequest request, @RequestBody ConfigModel config) {
        logger.info("[changeConfig] Request IP: {}, Config Change To: {}", request.getRemoteAddr(), config);
        String sourceIp = request.getHeader("X-FORWARDED-FOR");
        if(sourceIp == null) {
            sourceIp = request.getRemoteAddr();
        }
        return changeConfig(config.getKey(), config.getVal(), sourceIp);
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

    private RetMessage changeConfig(final String key, final String val, final String uri) {
        UserInfo user = UserInfoHolder.DEFAULT.getUser();
        String userId = user.getUserId();
        ConfigModel configModel = new ConfigModel();
        configModel.setUpdateUser(userId);
        configModel.setUpdateIP(uri);
        logger.info("[changeConfig] Config changed by user: {} and ip: {}", userId, uri);
        try {
            boolean target = Boolean.parseBoolean(val);
            if(KEY_ALERT_SYSTEM_ON.equalsIgnoreCase(key)) {
                if(target) {
                    configService.startAlertSystem(configModel);
                } else {
                    configService.stopAlertSystem(configModel, consoleConfig.getConfigDefaultRestoreHours());
                }
            } else if(KEY_SENTINEL_AUTO_PROCESS.equalsIgnoreCase(key)) {
                if(target) {
                    configService.startSentinelAutoProcess(configModel);
                } else {
                    configService.stopSentinelAutoProcess(configModel, consoleConfig.getConfigDefaultRestoreHours());
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
