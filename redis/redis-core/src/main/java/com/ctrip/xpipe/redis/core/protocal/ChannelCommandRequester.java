package com.ctrip.xpipe.redis.core.protocal;

import com.ctrip.xpipe.exception.XpipeException;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * May 12, 2016 9:22:03 AM
 */
public interface ChannelCommandRequester {

	
	void request(Command command);
	
	void handleResponse(ByteBuf byteBuf) throws XpipeException;

	void connectionClosed();
}
