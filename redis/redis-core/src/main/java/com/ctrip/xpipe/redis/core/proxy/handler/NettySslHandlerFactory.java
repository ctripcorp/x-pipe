package com.ctrip.xpipe.redis.core.proxy.handler;

import io.netty.channel.ChannelHandler;

/**
 * @author chen.zhu
 * <p>
 * May 11, 2018
 */
public interface NettySslHandlerFactory {

    ChannelHandler createSslHandler();
}
