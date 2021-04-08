package com.ctrip.xpipe.redis.integratedtest.console;

/**
 * @author lishanglin
 * date 2021/1/25
 */
public interface ForkProcessCmd {

    void killProcess();

    boolean isProcessAlive();

}
