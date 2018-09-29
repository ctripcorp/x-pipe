package com.ctrip.xpipe.redis.core.proxy.handler;

import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;

/**
 * @author chen.zhu
 * <p>
 * May 11, 2018
 */
public interface NettySslHandlerFactory {

    SslHandler createSslHandler(SocketChannel channel);
}
