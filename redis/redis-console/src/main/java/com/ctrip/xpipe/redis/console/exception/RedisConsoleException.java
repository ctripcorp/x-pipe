package com.ctrip.xpipe.redis.console.exception;

import com.ctrip.xpipe.redis.core.exception.RedisException;

/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class RedisConsoleException extends RedisException{
	private static final long serialVersionUID = 1L;

	public RedisConsoleException(String message) {
		super(message);
	}
	
	public RedisConsoleException(String msg, Throwable th) {
		super(msg, th);
	}
}
