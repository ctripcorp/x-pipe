package com.ctrip.xpipe.redis.console.healthcheck.nonredis.console;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import org.springframework.stereotype.Component;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_KEEPER_BALANCE_INFO_COLLECT;

@Component
public class KeeperBalanceInfoCollectOffChecker extends AbstractConsoleHealthChecker {
    @Override
    boolean stop() {
        return configService.isKeeperBalanceInfoCollectOn();
    }

    @Override
    void alert() {
        ConfigModel config = configService.getConfig(KEY_KEEPER_BALANCE_INFO_COLLECT);

        String user = config.getUpdateUser() == null ? "unkown" : config.getUpdateUser();
        String ip = config.getUpdateIP() == null ? "unkown" : config.getUpdateIP();
        String message = String.format("Keeper balance info collect will be online %s, </br> " +
                        "Recent update by： %s <br/> and from IP： %s",
                configService.getKeeperBalanceInfoCollectRecoverTime().toString(),
                user, ip);
        alertManager.alert("", "", null, ALERT_TYPE.KEEPER_BALANCE_INFO_COLLECT_OFF, message);
    }
}
