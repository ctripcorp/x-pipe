package com.ctrip.xpipe.redis.console.exception;

/**
 * http code:400
 * @author shyin
 *
 * Aug 15, 2016
 */
public class BadRequestException extends RedisConsoleRuntimeException{
	private static final long serialVersionUID = 1L;


	public BadRequestException(String message) {
		super(message);
	}

	public BadRequestException(String msg, Throwable th) {
		super(msg, th);
	}
}
