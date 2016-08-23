package com.ctrip.xpipe.redis.console.exception;

/**
 * http code: 404
 * @author shyin
 *
 * Aug 10, 2016
 */
public class DataNotFoundException extends RedisConsoleRuntimeException{
	private static final long serialVersionUID = 1L;


	public DataNotFoundException(String message) {
		super(message);
	}

	public DataNotFoundException(String msg, Throwable th) {
		super(msg, th);
	}
}
