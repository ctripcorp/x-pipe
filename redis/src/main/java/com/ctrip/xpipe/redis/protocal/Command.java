package com.ctrip.xpipe.redis.protocal;


import com.ctrip.xpipe.exception.XpipeException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;



/**
 * 
 * @author wenchao.meng
 *
 * 2016年3月24日 上午11:32:27
 */
public interface Command {
	
	String getName();
	
	void request(Channel channel) throws XpipeException;
	
	/**
	 * @param byteBuf
	 * @return true，代表
	 * @throws XpipeException 
	 */
	RESPONSE_STATE handleResponse(Channel channel, ByteBuf byteBuf) throws XpipeException;
	
	/**
	 * do something if connection is closed, but we has not finished reading or writing
	 */
	void connectionClosed();
	
	/**
	 * reset this command, so it can be used again
	 */
	void reset();
	
	public static enum RESPONSE_STATE{
		SUCCESS,
		FAIL_CONTINUE,
		FAIL_STOP,
		GO_ON_READING_BUF//go on giving bytebuf
	}
}