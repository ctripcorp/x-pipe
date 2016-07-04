package com.ctrip.xpipe.api.command;

import java.util.concurrent.TimeUnit;

import com.ctrip.xpipe.command.CommandExecutionException;

/**
 * @author wenchao.meng
 *
 * Jun 30, 2016
 */
public interface Command<V> {
	
	CommandFuture<V> execute() throws CommandExecutionException;
	
	CommandFuture<V>  execute(int time, TimeUnit timeUnit);
	
	String getName();

}
