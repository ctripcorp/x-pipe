package com.ctrip.xpipe.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public interface ListenableFuture<V> extends Future<V>{

	boolean isSuccess();
	
	Throwable cause();
		
	void setSuccess(V result);
	
	/**
	 * result null
	 */
	void setSuccess();
	
	void setFailure(Throwable cause);
	
	ListenableFuture<V> sync() throws InterruptedException, ExecutionException;

    Future<V> await() throws InterruptedException;

    boolean await(long timeout, TimeUnit unit) throws InterruptedException;
    
    void addListener(FutureListener<? super ListenableFuture<? super V>> futureListener);
    
    V getNow();


}
