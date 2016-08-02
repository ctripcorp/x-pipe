package com.ctrip.xpipe.netty.commands;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public interface ByteBufReceiver {

	boolean receive(Channel channel, ByteBuf byteBuf);
	
	void clientClosed(NettyClient nettyClient);
}
