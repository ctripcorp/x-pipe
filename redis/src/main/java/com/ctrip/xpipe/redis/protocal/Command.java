package com.ctrip.xpipe.redis.protocal;


import com.ctrip.xpipe.exception.XpipeException;

import io.netty.buffer.ByteBuf;



/**
 * @author wenchao.meng
 *
 * 2016年3月24日 上午11:32:27
 */
public interface Command {
	
	String getName();
	
	void request() throws XpipeException;
	
	/**
	 * @param byteBuf
	 * @return true，代表
	 * @throws XpipeException 
	 */
	RESPONSE_STATE handleResponse(ByteBuf byteBuf) throws XpipeException;
	
	
	public static enum RESPONSE_STATE{
		SUCCESS,
		FAIL_CONTINUE,
		FAIL_STOP,
		CONTINUE//go on giving bytebuf
	}
	
}