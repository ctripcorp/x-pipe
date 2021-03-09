package com.ctrip.xpipe.redis.checker.config;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/13
 */
public interface CheckerDbConfig {

    boolean isAlertSystemOn();

    boolean isSentinelAutoProcess();

    boolean shouldSentinelCheck(String cluster);

    Set<String> sentinelCheckWhiteList();

}
