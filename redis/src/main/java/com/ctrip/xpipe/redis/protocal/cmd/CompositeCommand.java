package com.ctrip.xpipe.redis.protocal.cmd;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.CmdContext;
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
	protected RESPONSE_STATE doHandleResponse(CmdContext cmdContext, ByteBuf byteBuf) throws XpipeException {
		
		RESPONSE_STATE responseState = currentCommand.handleResponse(cmdContext, byteBuf);
		
		RESPONSE_STATE retState = responseState;
		switch(responseState){
		
			case FAIL_CONTINUE:
			case SUCCESS:
				Command command = requestNextCommand(cmdContext);
				if(command == null){
					retState = RESPONSE_STATE.SUCCESS;
				}else{
					retState = RESPONSE_STATE.GO_ON_READING_BUF;
				}
				break;
			case GO_ON_READING_BUF:
				break;
			case FAIL_STOP:
				retState = RESPONSE_STATE.FAIL_CONTINUE;
				break;
		}
		
		return retState;
	}
	
	private Command requestNextCommand(CmdContext cmdContext) throws XpipeException {
		
		Command command = nextCommand();
		if(command != null){
			cmdContext.sendCommand(command);
		}
		return command;
	}


	private Command nextCommand() {
		
		int index = currentIndex.incrementAndGet();
		
		if( index >= commands.size()){
			
			if(logger.isInfoEnabled()){
				logger.info("[requestNextCommand][game over]");
			}
			return null;
		}
		
		currentCommand = commands.get(index);
		return currentCommand;
	}

	@Override
	protected ByteBuf doRequest() {
		
		return nextCommand().request();
	}

	@Override
	protected void doConnectionClosed() {
		currentCommand.connectionClosed();
	}

	@Override
	protected boolean hasResponse() {
		return true;
	}

	@Override
	protected void doReset() {
		
		currentIndex.set(-1);
		for(Command command : commands){
			command.reset();
		}
	}
}
