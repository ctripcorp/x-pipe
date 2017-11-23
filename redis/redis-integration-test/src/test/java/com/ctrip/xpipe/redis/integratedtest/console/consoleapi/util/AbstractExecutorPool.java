package com.ctrip.xpipe.redis.integratedtest.console.consoleapi.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author liuyi
 * 
 *         Sep 9, 2016
 */
public abstract class AbstractExecutorPool {
	protected ExecutorService fixedThreadPool;

	public AbstractExecutorPool() {
		init();
	}

	public void addThread(final String apiName) {
		fixedThreadPool.execute(new Runnable() {
			public void run() {
				test();
			}
		});
	}

	private void init() {
		fixedThreadPool = Executors.newFixedThreadPool(getPoolSize());
	}

	abstract protected void test();

	abstract protected int getPoolSize();
}
