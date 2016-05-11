package com.ctrip.xpipe.redis.protocal.cmd;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.Command;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午4:24:48
 */
public class CompositeCommand extends AbstractCommand{
	
	private List<Command> commands = new ArrayList<Command>();
	
	private AtomicInteger currentIndex = new AtomicInteger(-1);
	private Command 	  currentCommand;

	public CompositeCommand(Command ... commands) {
		for(Command command : commands){
			this.commands.add(command);
		}
	}

	@Override
	public String getName() {
		
		return "composite:" + commands;
	}


	@Override
	protected RESPONSE_STATE doHandleResponse(ByteBuf byteBuf) throws XpipeException {
		
		RESPONSE_STATE responseState = currentCommand.handleResponse(byteBuf);
		
		switch(responseState){
		
			case FAIL_CONTINUE:
			case SUCCESS:
				requestNextCommand();
				break;
			case CONTINUE:
				break;
			case FAIL_STOP:
				break;
		}
		
		return responseState;
	}
	
	private void requestNextCommand() throws XpipeException {
		
		int index = currentIndex.incrementAndGet();
		
		if( index >= commands.size()){
			
			if(logger.isInfoEnabled()){
				logger.info("[requestNextCommand][game over]");
			}
			return;
		}
		
		currentCommand = commands.get(index);
		currentCommand.request();
	}


	@Override
	protected void doRequest() throws XpipeException {
		requestNextCommand();
	}

	@Override
	protected void doConnectionClosed() {
		currentCommand.connectionClosed();
	}
}
