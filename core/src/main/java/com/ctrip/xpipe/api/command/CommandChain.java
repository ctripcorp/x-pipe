package com.ctrip.xpipe.api.command;

/**
 * @author wenchao.meng
 *
 * Jul 15, 2016
 */
public interface CommandChain<V> extends Command<V>{
	
	void add(Command<?> command);
	
	void remove(Command<?> command);
	
	int executeCount();

}
