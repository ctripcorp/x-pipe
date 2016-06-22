package com.ctrip.xpipe.redis.keeper.exception;

import com.ctrip.xpipe.redis.core.exception.RedisException;

/**
 * @author wenchao.meng
 *
 * Jun 22, 2016
 */
public class RedisKeeperException extends RedisException{

	private static final long serialVersionUID = 1L;

	public RedisKeeperException(String message) {
		super(message);
	}
	

}
