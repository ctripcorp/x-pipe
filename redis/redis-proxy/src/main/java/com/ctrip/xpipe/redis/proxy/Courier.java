package com.ctrip.xpipe.redis.proxy;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface Courier extends Lifecycle {

    ChannelFuture deliver(ByteBuf message);

    void addCommand(Command command);

    void setDestation(Session dst);

}
