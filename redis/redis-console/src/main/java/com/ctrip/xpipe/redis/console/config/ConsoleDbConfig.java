package com.ctrip.xpipe.redis.console.config;

import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;

import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
public interface ConsoleDbConfig extends CheckerDbConfig {

    boolean shouldSentinelCheck(String cluster, boolean disableCache);

    Set<String> sentinelCheckWhiteList(boolean disableCache);

}
