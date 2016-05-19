package com.ctrip.xpipe.redis.protocal.cmd;

import java.nio.charset.Charset;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.Command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午12:04:13
 */
public abstract class AbstractCommand implements Command{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	protected Charset charset = Charset.forName("UTF-8");
	
	protected AbstractCommand(){
	}
	@Override
	public void request(Channel channel) throws XpipeException {
		doRequest(channel);
	}
	
	protected abstract boolean hasResponse();
	
	protected abstract void doRequest(Channel channel) throws XpipeException;

	@Override
	public RESPONSE_STATE handleResponse(Channel channel, ByteBuf byteBuf) throws XpipeException {
		if(!hasResponse()){
			return RESPONSE_STATE.SUCCESS;
		}
		
		return doHandleResponse(channel, byteBuf);
	}
	
	protected abstract RESPONSE_STATE doHandleResponse(Channel channel, ByteBuf byteBuf) throws XpipeException;
	
	
	@Override
	public void connectionClosed() {
		doConnectionClosed();
	}

	protected abstract void doConnectionClosed();
	
	@Override
	public void reset() {
		doReset();
	}
	
	protected abstract void doReset();
}

