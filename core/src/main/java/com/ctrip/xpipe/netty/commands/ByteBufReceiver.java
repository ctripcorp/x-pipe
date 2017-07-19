package com.ctrip.xpipe.netty.commands;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public interface ByteBufReceiver {

	RECEIVER_RESULT receive(Channel channel, ByteBuf byteBuf);
	
	void clientClosed(NettyClient nettyClient);


	public static enum RECEIVER_RESULT{

		SUCCESS,
		FAIL,
		CONTINUE,
		ALREADY_FINISH
	}
}
