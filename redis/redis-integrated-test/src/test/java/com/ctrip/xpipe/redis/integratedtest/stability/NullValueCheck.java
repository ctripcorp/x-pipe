package com.ctrip.xpipe.redis.integratedtest.stability;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.tuple.Pair;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;

/**
 * @author wenchao.meng
 *
 * Nov 14, 2016
 */
public class NullValueCheck extends AbstractStartStoppable implements ValueCheck{
	
	private AtomicLong totalQueue = new AtomicLong();

	@Override
	public void offer(Pair<String, String> checkData) {
		totalQueue.incrementAndGet();
		
	}

	@Override
	public long queueSize() {
		return totalQueue.get();
	}

	@Override
	protected void doStart() throws Exception {
		
	}

	@Override
	protected void doStop() {
		
	}

}
