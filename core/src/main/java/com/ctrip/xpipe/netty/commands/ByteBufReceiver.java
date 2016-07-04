package com.ctrip.xpipe.netty.commands;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public interface ByteBufReceiver {

	boolean receive(ByteBuf byteBuf);
	
	void clientClosed(NettyClient nettyClient);
}
