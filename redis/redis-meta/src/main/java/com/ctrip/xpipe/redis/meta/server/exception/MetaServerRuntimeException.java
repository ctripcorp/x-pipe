package com.ctrip.xpipe.redis.meta.server.exception;

import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;

/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class MetaServerRuntimeException extends RedisRuntimeException{

	private static final long serialVersionUID = 1L;

	public MetaServerRuntimeException(String message) {
		super(message);
	}
	
	public MetaServerRuntimeException(String msg, Throwable th) {
		super(msg, th);
	}
}
