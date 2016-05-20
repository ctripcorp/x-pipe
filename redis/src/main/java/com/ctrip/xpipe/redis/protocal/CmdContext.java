package com.ctrip.xpipe.redis.protocal;

import java.util.concurrent.TimeUnit;

import io.netty.channel.Channel;

/**
 * @author wenchao.meng
 *
 * May 20, 2016 1:57:58 PM
 */
public class CmdContext {
	
	private Channel channel;
	
	private CommandRequester commandRequester;
	
	public CmdContext(Channel channel, CommandRequester commandRequester){
		
		this.channel = channel;
		this.commandRequester = commandRequester;
	}
	
	public void sendCommand(Command command){
		
		commandRequester.request(channel, command);
	}
	
	public void schedule(TimeUnit timeunit, int delay, Command command){
		commandRequester.schedule(timeunit, delay, channel, command);
	}
	
	@Override
	public String toString() {
		return this.channel.toString();
	}
}
