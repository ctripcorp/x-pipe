package com.ctrip.xpipe.redis.console.healthcheck.nonredis.console;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/5/13
 */
@Component
public class AutoMigrationOffChecker extends AbstractConsoleHealthChecker {

    @Override
    boolean stop() {
        return configService.allowAutoMigration();
    }

    @Override
    void alert() {
        String message = "Auto Migration Not Allow";
        logger.info("[alert] sending alert: {}", message);
        alertManager.alert("", "", null, ALERT_TYPE.AUTO_MIGRATION_NOT_ALLOW, message);
    }

}
