package com.ctrip.xpipe.redis.protocal.cmd;



import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.protocal.ChannelCommandRequester;
import com.ctrip.xpipe.redis.protocal.Command;
import com.ctrip.xpipe.redis.protocal.Command.RESPONSE_STATE;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * May 12, 2016 9:16:52 AM
 */
public class SequenceChannelCommandRequester implements ChannelCommandRequester{
	
	private Channel channel;
	
	private BlockingQueue<Command> commands = new LinkedBlockingQueue<Command>();
	
	private Command currentCommand;
	
	private Object currentCommandLock = new Object();
	
	
	
	public SequenceChannelCommandRequester(Channel channel){
		
		this.channel = channel;
	}

	public void request(Command command) throws XpipeException {
		
		commands.offer(command);
		
		synchronized (currentCommandLock) {
			if(currentCommand == null){
				requestNextCommand();
			}
			
		}
	}

	public void handleResponse(ByteBuf byteBuf) throws XpipeException {
		
		if(currentCommand == null){
			throw new IllegalStateException("current command == null");
		}
		
		RESPONSE_STATE responseState = currentCommand.handleResponse(channel, byteBuf);
		switch(responseState){
		
			case SUCCESS:
			case FAIL_CONTINUE:
				requestNextCommand();
				break;
			case GO_ON_READING_BUF:
			case FAIL_STOP:
				break;
		}
	}

	private void requestNextCommand() throws XpipeException {
		
		currentCommand = commands.poll();
		if(currentCommand != null){
			currentCommand.request(channel);
		}
	}

	@Override
	public void connectionClosed() {
		
		if(currentCommand != null){
			currentCommand.connectionClosed();
		}
	}
}
