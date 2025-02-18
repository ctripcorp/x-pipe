package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;

import java.util.Collections;
import java.util.Set;

/**
 * @author lishanglin
 * date 2024/7/11
 */
public class TestCommonConfig implements ConsoleCommonConfig {

    @Override
    public Set<String> getBeaconSupportZones() {
        return Collections.emptySet();
    }

    @Override
    public int monitorUnregisterProtectCount() {
        return 10;
    }

    @Override
    public boolean isKeeperMsgCollectOn() {
        return true;
    }

}
