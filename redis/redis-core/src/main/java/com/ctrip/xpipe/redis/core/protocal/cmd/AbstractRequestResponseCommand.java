package com.ctrip.xpipe.redis.core.protocal.cmd;

import java.io.IOException;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.core.protocal.CmdContext;
import com.ctrip.xpipe.redis.core.protocal.RequestResponseCommand;
import com.ctrip.xpipe.redis.core.protocal.RequestResponseCommandListener;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * May 11, 2016 5:49:04 PM
 */
public abstract class AbstractRequestResponseCommand extends AbstractCommand implements RequestResponseCommand{
	
	private RequestResponseCommandListener commandListener;
	private REQUEST_RESPONSE_STATE state = REQUEST_RESPONSE_STATE.NONE;
	
	@Override
	public ByteBuf request() {
		
		ByteBuf result = super.request();
		state = REQUEST_RESPONSE_STATE.WAIT_FOR_RESPONSE;
		return result;
	}

	@Override
	protected RESPONSE_STATE doHandleResponse(CmdContext cmdContext, ByteBuf byteBuf) throws XpipeException {

		Object result = readResponse(byteBuf);
		if(result == null){
			return RESPONSE_STATE.GO_ON_READING_BUF;
		}

		state = REQUEST_RESPONSE_STATE.GOT_RESPONSE;

		if(commandListener != null){
			try{
				if(result instanceof Exception){
					commandListener.onComplete(cmdContext, null, (Exception)result);
				}else{
					commandListener.onComplete(cmdContext, result, null);
				}
			}catch(Exception e){
				logger.error("[doHandleResponse]" + cmdContext, e);
			}		
		}
		return RESPONSE_STATE.SUCCESS;
	}

	protected abstract Object readResponse(ByteBuf byteBuf) throws XpipeException;
	
	@Override
	public void setCommandListener(RequestResponseCommandListener commandListener) {
		this.commandListener = commandListener;
	}
	
	@Override
	protected boolean hasResponse() {
		
		return true;
	}

	@Override
	public void connectionClosed() {
		
		if(state == REQUEST_RESPONSE_STATE.WAIT_FOR_RESPONSE){
			if(commandListener != null){
				commandListener.onComplete(null, null, new IOException("connection closed"));
			}
		}
		super.connectionClosed();
	}
	
	public static enum REQUEST_RESPONSE_STATE{
		NONE,
		WAIT_FOR_RESPONSE,
		GOT_RESPONSE
	}
}
