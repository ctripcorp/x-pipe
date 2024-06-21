package com.ctrip.xpipe.redis.console.healthcheck.nonredis.console;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import org.springframework.stereotype.Component;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_SENTINEL_AUTO_PROCESS;

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
        ConfigModel config = configService.getConfig(KEY_SENTINEL_AUTO_PROCESS);

        String user = config.getUpdateUser() == null ? "unkown" : config.getUpdateUser();
        String ip = config.getUpdateIP() == null ? "unkown" : config.getUpdateIP();
        String message = String.format("Sentinel Auto Process will be online %s, </br> " +
                        "Recent update by： %s <br/> and from IP： %s",
                configService.getSentinelAutoProcessRecoverTime().toString(),
                user, ip);
        alertManager.alert("", "", null, ALERT_TYPE.SENTINEL_AUTO_PROCESS_OFF, message);
    }
}
