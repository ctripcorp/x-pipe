package com.ctrip.xpipe.redis.console.health.console;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfig;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

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
        ConfigModel config = configService.getConfig(DefaultConsoleDbConfig.KEY_ALERT_SYSTEM_ON);
        String message = String.format("Alert System will be online %s, latest updated by %s or ip %s or ip %s",
                configService.getAlertSystemRecoverTime().toString(),
                config.getUpdateUser(), config.getUpdateIP());
        logger.info("[alert] sending alert: {}", message);
        alertManager.alert("", "", null, ALERT_TYPE.ALERT_SYSTEM_OFF, message);
    }
}
