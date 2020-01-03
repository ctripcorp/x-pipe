package com.ctrip.xpipe.redis.console.config;

import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
public interface ConsoleDbConfig {


    boolean isSentinelAutoProcess();

    boolean isAlertSystemOn();

    boolean ignoreMigrationSystemAvailability();

    boolean shouldSentinelCheck(String cluster, boolean disableCache);

    Set<String> sentinelCheckWhiteList(boolean disableCache);

}
