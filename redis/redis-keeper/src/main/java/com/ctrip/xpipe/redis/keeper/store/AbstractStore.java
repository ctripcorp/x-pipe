package com.ctrip.xpipe.redis.keeper.store;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author wenchao.meng
 *
 * Jan 20, 2017
 */
public abstract class AbstractStore {
	
	private AtomicBoolean isClosed = new AtomicBoolean(false);
	
	
	protected boolean cmpAndSetClosed(){
		return isClosed.compareAndSet(false, true);
	}
	
	public void makeSureOpen(){
		
		if(isClosed.get()){
			throw new IllegalStateException("[makeSureOpen][closed]" + this);
		}
	}
	
	public boolean isClosed() {
		return isClosed.get();
	}
	
}
