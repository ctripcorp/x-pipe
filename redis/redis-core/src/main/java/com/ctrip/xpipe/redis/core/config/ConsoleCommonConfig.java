package com.ctrip.xpipe.redis.core.config;

import java.util.Set;

/**
 * @author lishanglin
 * date 2024/7/11
 */
public interface ConsoleCommonConfig {

    // Beacon
    Set<String> getBeaconSupportZones();
    int monitorUnregisterProtectCount();
}
