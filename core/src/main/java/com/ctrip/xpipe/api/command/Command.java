package com.ctrip.xpipe.api.command;


import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * @author wenchao.meng
 *
 * Jun 30, 2016
 */
public interface Command<V> {
	
	CommandFuture<V> future();
	
	CommandFuture<V> execute() ;

	CommandFuture<V> execute(ExecutorService executors) ;

	String getName();
	
	/**
	 * 重置，可以重新执行
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	void reset();

}
