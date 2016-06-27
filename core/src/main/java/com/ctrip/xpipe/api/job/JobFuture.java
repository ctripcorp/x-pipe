package com.ctrip.xpipe.api.job;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *
 * Jun 26, 2016
 */
public interface JobFuture<V> extends Future<V>{
	
	
	boolean isSuccess();
	
	Throwable cause();
		
	void setSuccess(V result);
	
	void setFailure(Throwable cause);
	
	void sync() throws InterruptedException, ExecutionException;

    Future<V> await() throws InterruptedException;

    boolean await(long timeout, TimeUnit unit) throws InterruptedException;
}
