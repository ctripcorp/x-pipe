package com.ctrip.framework.xpipe.redis.utils;

import com.ctrip.framework.xpipe.redis.ProxyChecker;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractProxyCheckerTest implements ProxyChecker {
    int retryUpTimes;
    int retryDownTimes;
    public AbstractProxyCheckerTest(int retryUpNum, int retryDownNum) {
        this.retryUpTimes = retryUpNum;
        this.retryDownTimes = retryDownNum;
    }

    @Override
    public int getRetryUpTimes() {
        return retryUpTimes;
    }

    @Override
    public int getRetryDownTimes() {
        return retryDownTimes;
    }
}
