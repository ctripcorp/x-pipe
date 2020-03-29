package com.ctrip.xpipe.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public interface NettyRequestListener {

    void onSend(Channel channel, ByteBuf request);

    void onTimeout(Channel channel, int timeoutMilli);

    void onReceive(Channel channel, ByteBuf response);

}
