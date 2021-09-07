package com.ctrip.framework.xpipe.redis.utils;

import com.ctrip.framework.xpipe.redis.ProxyChecker;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractProxyCheckerTest implements ProxyChecker {
    int retryUpNum;
    int retryDownNum;
    public AbstractProxyCheckerTest(int retryUpNum, int retryDownNum) {
        this.retryUpNum = retryUpNum;
        this.retryDownNum = retryDownNum;
    }

    @Override
    public int getRetryUpNum() {
        return retryUpNum;
    }

    @Override
    public int getRetryDownNum() {
        return retryDownNum;
    }
}
