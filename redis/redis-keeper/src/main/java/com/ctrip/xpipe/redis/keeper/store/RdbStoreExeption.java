package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.keeper.exception.RedisKeeperRuntimeException;

/**
 * @author wenchao.meng
 *
 * Aug 24, 2016
 */
public class RdbStoreExeption extends RedisKeeperRuntimeException{

	private static final long serialVersionUID = 1L;

	public RdbStoreExeption(long expectedSize, long realSize) {
		super(String.format("expected:%d, real:%d", expectedSize, realSize));
	}

}
