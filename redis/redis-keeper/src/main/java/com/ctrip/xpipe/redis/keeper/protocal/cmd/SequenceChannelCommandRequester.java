package com.ctrip.xpipe.redis.keeper.protocal.cmd;



import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.redis.keeper.protocal.ChannelCommandRequester;
import com.ctrip.xpipe.redis.keeper.protocal.CmdContext;
import com.ctrip.xpipe.redis.keeper.protocal.Command;
import com.ctrip.xpipe.redis.keeper.protocal.CommandRequester;
import com.ctrip.xpipe.redis.keeper.protocal.Command.RESPONSE_STATE;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * May 12, 2016 9:16:52 AM
 */
public class SequenceChannelCommandRequester implements ChannelCommandRequester{
	
	private BlockingQueue<Command> commands = new LinkedBlockingQueue<Command>();
	
	private Command currentCommand;
	
	private Object currentCommandLock = new Object();
	
	private Channel channel;
	private CommandRequester commandRequester;
	
	public SequenceChannelCommandRequester(Channel channel, CommandRequester commandRequester){
		
		this.channel = channel;
		this.commandRequester = commandRequester;
		
	}

	public void request(Command command) {
		
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
		
		RESPONSE_STATE responseState = currentCommand.handleResponse(new CmdContext(channel, commandRequester), byteBuf);
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

	private void requestNextCommand() {
		
		currentCommand = commands.poll();
		if(currentCommand != null){
			channel.writeAndFlush(currentCommand.request());
		}
	}

	@Override
	public void connectionClosed() {
		
		if(currentCommand != null){
			currentCommand.connectionClosed();
		}
	}
}
