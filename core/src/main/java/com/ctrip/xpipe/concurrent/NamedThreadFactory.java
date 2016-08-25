package com.ctrip.xpipe.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午3:28:41
 */
public class NamedThreadFactory implements ThreadFactory{
	
	private String prefix;
	
	private AtomicInteger threadIndex = new AtomicInteger();
	
	public NamedThreadFactory(String prefix) {
		this.prefix = prefix;
	}

	@Override
	public Thread newThread(Runnable r) {
		
		Thread thread = new Thread(r);
		thread.setName(prefix + "-" + threadIndex.incrementAndGet());
		return thread;
	}
	
}
