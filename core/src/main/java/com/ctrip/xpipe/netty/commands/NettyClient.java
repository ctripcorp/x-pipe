package com.ctrip.xpipe.netty.commands;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public interface NettyClient {
	
	void sendRequest(ByteBuf byteBuf);
	
	/**
	 * @param byteBuf
	 * @return if true, means finish reading
	 */
	void handleResponse(ByteBuf byteBuf);
	
	Channel channel();

	/**
	 * @param byteBuf
	 * @param byteBufReceiver
	 */
	void sendRequest(ByteBuf byteBuf, ByteBufReceiver byteBufReceiver);
	
}
