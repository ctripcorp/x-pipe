package com.ctrip.xpipe.redis.keeper.simple;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public abstract class AbstractRedis extends AbstractLifecycle{
	
	protected ExecutorService executors = Executors.newCachedThreadPool();
	
	protected ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(4);

	
	protected InetSocketAddress master = new InetSocketAddress("127.0.0.1", 6379);

	public AbstractRedis(InetSocketAddress master){
		this.master = master;
	}

}
