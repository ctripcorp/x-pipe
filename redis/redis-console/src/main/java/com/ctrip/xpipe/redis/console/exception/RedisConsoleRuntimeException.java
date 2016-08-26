package com.ctrip.xpipe.redis.console.exception;

import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;

/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class RedisConsoleRuntimeException extends RedisRuntimeException{
	private static final long serialVersionUID = 1L;

	public RedisConsoleRuntimeException(String message) {
		super(message);
	}
	
	public RedisConsoleRuntimeException(String msg, Throwable th) {
		super(msg, th);
	}
}
