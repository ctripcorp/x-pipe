package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.keeper.RedisKeeperServerState;

/**
 * @author wenchao.meng
 *
 * Jun 12, 2016
 */
public class KeeperServerStateChanged {

	private final RedisKeeperServerState previous, current;
	
	public KeeperServerStateChanged(RedisKeeperServerState previous, RedisKeeperServerState current) {
		this.previous = previous;
		this.current = current;
	}
	public RedisKeeperServerState getPrevious() {
		return previous;
	}
	public RedisKeeperServerState getCurrent() {
		return current;
	}
	
	@Override
	public String toString() {
		return previous + "->" + current;
	}

}
