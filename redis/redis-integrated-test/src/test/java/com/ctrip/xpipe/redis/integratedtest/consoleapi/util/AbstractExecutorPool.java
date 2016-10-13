package com.ctrip.xpipe.redis.integratedtest.consoleapi.util;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author liuyi
 * 
 *         Sep 9, 2016
 */
public abstract class AbstractExecutorPool{
	protected ExecutorService fixedThreadPool;

	public AbstractExecutorPool() {
		fixedThreadPool =new ThreadPoolExecutor(5, 20,5, TimeUnit.MINUTES, 
		        new ArrayBlockingQueue<Runnable>(200),
		        new DefaultThreadFactory("defaultAbstractExecutorPool")) {
				    protected void afterExecute(Runnable r, Throwable t) {
				        super.afterExecute(r, t);
				        threadExceptionHandler(r,t);
				    }
				};
	}

	public void addThread(final String apiName) {
		fixedThreadPool.execute(new Thread() {
			public void run() {
				test();
			}
		});
	}

	abstract protected void test();

	abstract protected int getPoolSize();
	
	abstract protected void threadExceptionHandler(Runnable r, Throwable t);
}
