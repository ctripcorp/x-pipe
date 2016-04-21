package com.ctrip.xpipe.redis.protocal.cmd;



import java.nio.charset.Charset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.Command;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午12:04:13
 */
public abstract class AbstractCommand implements Command{
	
	protected Logger logger = LogManager.getLogger();

	protected Charset charset = Charset.forName("UTF-8");
	
	protected AbstractCommand(){
	}
	@Override
	public void request() throws XpipeException {
		doRequest();
	}
	
	protected boolean hasResponse() {
		return true;
	}

	protected abstract void doRequest() throws XpipeException;

	@Override
	public RESPONSE_STATE handleResponse(ByteBuf byteBuf) throws XpipeException {
		if(!hasResponse()){
			return RESPONSE_STATE.SUCCESS;
		}
		
		return doHandleResponse(byteBuf);
	}
	
	protected abstract RESPONSE_STATE doHandleResponse(ByteBuf byteBuf) throws XpipeException;
}
