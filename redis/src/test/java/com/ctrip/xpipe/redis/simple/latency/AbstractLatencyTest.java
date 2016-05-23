package com.ctrip.xpipe.redis.simple.latency;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public class AbstractLatencyTest extends AbstractLifecycle{
	
	
	protected ExecutorService executors = Executors.newCachedThreadPool();
	
	protected InetSocketAddress master = new InetSocketAddress("127.0.0.1", 6379);

//	private InetSocketAddress dest = new InetSocketAddress("127.0.0.1", 6479);

	protected InetSocketAddress dest = new InetSocketAddress("127.0.0.1", 7777);
	
	protected final long total = 1 << 30;
	
	protected final AtomicLong current = new AtomicLong();
	
	public AbstractLatencyTest(InetSocketAddress master, InetSocketAddress dest){
		this.master = master;
		this.dest = dest;
		
	}
	
	public long increase(){
		
		long next = current.incrementAndGet();
		if(next > total){
			current.decrementAndGet();
			return -1;
		}
		return next; 
	}
	


}
