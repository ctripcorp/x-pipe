package com.ctrip.xpipe.redis.core.protocal;

import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.exception.XpipeException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * send command to remote server
 * @author wenchao.meng
 *
 * May 11, 2016 6:14:40 PM
 */
public interface CommandRequester {
	
	void request(Channel channel, Command command);

	void schedule(TimeUnit timeunit, int delay, Channel channel, Command command);
	
	void handleResponse(Channel channel, ByteBuf byteBuf) throws XpipeException;

	void connectionClosed(Channel channel);
}
