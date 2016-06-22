package com.ctrip.xpipe.redis.core.protocal;


import com.ctrip.xpipe.exception.XpipeException;

import io.netty.buffer.ByteBuf;



/**
 * 
 * @author wenchao.meng
 *
 * 2016年3月24日 上午11:32:27
 */
public interface Command {
	
	String getName();
	
	ByteBuf request();
	
	/**
	 * @param byteBuf
	 * @return true，代表
	 * @throws XpipeException 
	 */
	RESPONSE_STATE handleResponse(CmdContext cmdContext, ByteBuf byteBuf) throws XpipeException;
	
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