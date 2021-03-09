package com.ctrip.xpipe.redis.checker.alert.sender.email.listener;

/**
 * @author chen.zhu
 * <p>
 * Mar 26, 2018
 */
public interface AsyncEmailSenderCallback {

    void success();

    void fail(Throwable throwable);
}
