package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;

/**
 * @author lishanglin
 * date 2024/7/11
 */
public class TestCommonConfig implements ConsoleCommonConfig {

    @Override
    public String getBeaconSupportZone() {
        return "";
    }

    @Override
    public int monitorUnregisterProtectCount() {
        return 10;
    }
}
