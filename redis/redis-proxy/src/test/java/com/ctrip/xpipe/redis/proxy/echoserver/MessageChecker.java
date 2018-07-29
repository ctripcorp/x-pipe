package com.ctrip.xpipe.redis.proxy.echoserver;

/**
 * @author chen.zhu
 * <p>
 * Jul 09, 2018
 */
public interface MessageChecker {
    void check(String message);

    void sendout(String message);

    void remove(String message);

    void start();
}
