package com.ctrip.xpipe.redis.console.healthcheck.nonredis.console;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_ALERT_SYSTEM_ON;

/**
 * @author chen.zhu
 * <p>
 * Nov 30, 2017
 */
@Component
@Lazy
public class AlertSystemOffChecker extends AbstractConsoleHealthChecker {

    @Override
    boolean stop() {
        return configService.isAlertSystemOn();
    }

    @Override
    void alert() {
        logger.info("[alert] Alert System Off");
        ConfigModel config = configService.getConfig(KEY_ALERT_SYSTEM_ON);
        String user = config.getUpdateUser() == null ? "unkown" : config.getUpdateUser();
        String ip = config.getUpdateIP() == null ? "unkown" : config.getUpdateIP();
        String message = String.format("Alert System will be online %s, </br> Recent update by： %s <br/> and from IP： %s",
                configService.getAlertSystemRecoverTime().toString(),
                user, ip);
        logger.info("[alert] sending alert: {}", message);
        alertManager.alert("", "", null, ALERT_TYPE.ALERT_SYSTEM_OFF, message);
    }
}
