package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperException;

/**
 * @author wenchao.meng
 *
 * Oct 18, 2016
 */
public abstract class AbstractRdbDumperException extends RedisKeeperException{
	
	private static final long serialVersionUID = 1L;
	private boolean cancelSlave = false;

	public AbstractRdbDumperException(String message, boolean cancelSlave){
		super(message);
		this.cancelSlave = cancelSlave;
	}
	
	
	public boolean isCancelSlave() {
		return cancelSlave;
	}

}
