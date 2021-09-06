package com.ctrip.framework.xpipe.redis;

import com.ctrip.xpipe.api.command.CommandFuture;

import java.net.InetSocketAddress;

/**
 * @author Slight
 * <p>
 * Sep 06, 2021 8:54 PM
 */
public interface ProxyChecker {

    CommandFuture<Boolean> check(InetSocketAddress address);
}
