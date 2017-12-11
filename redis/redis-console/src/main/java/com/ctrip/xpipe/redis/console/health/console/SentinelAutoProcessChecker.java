package com.ctrip.xpipe.redis.console.health.console;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfig;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.redis.console.service.ConfigService;
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
        ConfigModel config = configService.getConfig(DefaultConsoleDbConfig.KEY_SENTINEL_AUTO_PROCESS);
        String message = String.format("Sentinel Auto Process will be online %s, latest updated by %s or ip %s",
                configService.getSentinelAutoProcessRecoverTime().toString(),
                config.getUpdateUser(), config.getUpdateIP());
        alertManager.alert("", "", null, ALERT_TYPE.SENTINEL_AUTO_PROCESS_OFF, message);
    }
}
