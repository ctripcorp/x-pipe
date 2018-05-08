package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import io.netty.channel.ChannelFuture;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface Proxy extends Lifecycle {

    ChannelFuture startServer();

}
