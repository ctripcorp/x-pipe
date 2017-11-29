package com.ctrip.xpipe.redis.console.health.console;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Nov 30, 2017
 */
@Component
public class SentinelAutoProcessChecker extends AbstractConsoleHealthChecker {

    @Override
    boolean stop() {
        return configService.isSentinelAutoProcess();
    }

    @Override
    void alert() {
        String message = String.format("Sentinel Auto Process will recover on %s",
                configService.getSentinelAutoProcessRecoverTime().toString());
        alertManager.alert("", "", null, ALERT_TYPE.SENTINEL_AUTO_PROCESS_OFF, message);
    }
}
