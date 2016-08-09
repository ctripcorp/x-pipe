package com.ctrip.xpipe.redis.console.exception;

/**
 * @author shyin
 *
 * Aug 10, 2016
 */
public class DataNotFoundException extends RedisConsoleRuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @param message
	 */
	public DataNotFoundException(String message) {
		super(message);
	}
	

}
