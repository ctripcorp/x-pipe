package com.ctrip.framework.xpipe.redis;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * @author Slight
 * <p>
 * Sep 06, 2021 8:54 PM
 */
public interface ProxyChecker {

    CompletableFuture<Boolean> check(InetSocketAddress address);

    int getRetryUpNum();
    
    int getRetryDownNum();
    
}
