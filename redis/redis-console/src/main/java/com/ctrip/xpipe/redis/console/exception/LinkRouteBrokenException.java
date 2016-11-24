package com.ctrip.xpipe.redis.console.exception;

public class LinkRouteBrokenException extends RedisConsoleRuntimeException{
	private static final long serialVersionUID = 1L;


	public LinkRouteBrokenException(String message) {
		super(message);
	}

	public LinkRouteBrokenException(String msg, Throwable th) {
		super(msg, th);
	}

}
