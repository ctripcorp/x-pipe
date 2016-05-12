package com.ctrip.xpipe.redis.protocal.cmd;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.Command;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

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
	protected RESPONSE_STATE doHandleResponse(Channel channel, ByteBuf byteBuf) throws XpipeException {
		
		RESPONSE_STATE responseState = currentCommand.handleResponse(channel, byteBuf);
		
		RESPONSE_STATE retState = responseState;
		switch(responseState){
		
			case FAIL_CONTINUE:
			case SUCCESS:
				Command command = requestNextCommand(channel);
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
	
	private Command requestNextCommand(Channel channel) throws XpipeException {
		
		int index = currentIndex.incrementAndGet();
		
		if( index >= commands.size()){
			
			if(logger.isInfoEnabled()){
				logger.info("[requestNextCommand][game over]");
			}
			return null;
		}
		
		currentCommand = commands.get(index);
		currentCommand.request(channel);
		return currentCommand;
	}


	@Override
	protected void doRequest(Channel channel) throws XpipeException {
		requestNextCommand(channel);
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
