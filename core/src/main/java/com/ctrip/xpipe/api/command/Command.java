package com.ctrip.xpipe.api.command;


import java.util.concurrent.Executor;

/**
 * @author wenchao.meng
 *
 * Jun 30, 2016
 */
public interface Command<V> {
	
	CommandFuture<V> future();
	
	CommandFuture<V> execute() ;

	CommandFuture<V> execute(Executor executors) ;

	String getName();
	
	/**
	 * 重置，可以重新执行
	 */
	void reset();

}
