package com.ctrip.xpipe.api.command;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


/**
 * @author wenchao.meng
 *
 * Jul 1, 2016
 */
public interface CommandFuture<V> extends Future<V>{
	
	Command<V> command();
	
	boolean isSuccess();
	
	Throwable cause();
		
	void setSuccess(V result);
	
	/**
	 * result null
	 */
	void setSuccess();
	
	void setFailure(Throwable cause);
	
	CommandFuture<V> sync() throws InterruptedException, ExecutionException;

    Future<V> await() throws InterruptedException;

    boolean await(long timeout, TimeUnit unit) throws InterruptedException;
    
    void addListener(CommandFutureListener<V> commandFutureListener);
    
    V getNow();

	V getOrHandle(long timeout, TimeUnit unit, Function<Throwable, V> handler);

}
