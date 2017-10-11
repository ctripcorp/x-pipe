package com.ctrip.xpipe.redis.keeper.simple.latency;


import com.ctrip.xpipe.redis.keeper.simple.AbstractLoadRedis;

import java.net.InetSocketAddress;


/**
 * @author wenchao.meng
 *
 * May 23, 2016
 */
public class AbstractLatencyTest extends AbstractLoadRedis{
	
//	private InetSocketAddress dest = new InetSocketAddress("127.0.0.1", 6479);

	protected InetSocketAddress dest = new InetSocketAddress("127.0.0.1", 7777);
	
	
	public AbstractLatencyTest(InetSocketAddress master, InetSocketAddress dest){
		super(master);
		this.dest = dest;
	}



}
