package com.ctrip.xpipe.redis.protocal.cmd;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.CmdContext;
import com.ctrip.xpipe.redis.protocal.RequestResponseCommand;
import com.ctrip.xpipe.redis.protocal.RequestResponseCommandListener;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * May 11, 2016 5:49:04 PM
 */
public abstract class AbstractRequestResponseCommand extends AbstractCommand implements RequestResponseCommand{
	
	private RequestResponseCommandListener commandListener;

	@Override
	protected RESPONSE_STATE doHandleResponse(CmdContext cmdContext, ByteBuf byteBuf) throws XpipeException {
		
		Object result = readResponse(byteBuf);
		if(result == null){
			return RESPONSE_STATE.GO_ON_READING_BUF;
		}

		
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

	
}
