package com.ctrip.xpipe.redis.console.config;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 15, 2017
 */
public interface ConsoleDbConfig {


    boolean isSentinelAutoProcess();

    boolean isAlertSystemOn();

    boolean ignoreMigrationSystemAvailability();

}
