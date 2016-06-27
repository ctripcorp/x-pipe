package com.ctrip.xpipe.redis.meta.server.exception;

import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;

/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class RedisMetaServerRuntimeException extends RedisRuntimeException{

	private static final long serialVersionUID = 1L;

	public RedisMetaServerRuntimeException(String message) {
		super(message);
	}
	
	public RedisMetaServerRuntimeException(String msg, Throwable th) {
		super(msg, th);
	}
}
