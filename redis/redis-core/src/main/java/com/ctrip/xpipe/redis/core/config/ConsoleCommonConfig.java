package com.ctrip.xpipe.redis.core.config;

/**
 * @author lishanglin
 * date 2024/7/11
 */
public interface ConsoleCommonConfig {

    // Beacon
    String getBeaconSupportZone();
    int monitorUnregisterProtectCount();
}
