package com.ctrip.xpipe.redis.console.health.console;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Nov 30, 2017
 */
@Component
public class AlertSystemOffChecker extends AbstractConsoleHealthChecker {

    @Override
    boolean stop() {
        return configService.isAlertSystemOn();
    }

    @Override
    void alert() {
        String message = String.format("Alert System will recover on %s",
                configService.getAlertSystemRecoverTime().toString());
        logger.info("[alert] sending alert: {}", message);
        alertManager.alert("", "", null, ALERT_TYPE.ALERT_SYSTEM_OFF, message);
    }
}
