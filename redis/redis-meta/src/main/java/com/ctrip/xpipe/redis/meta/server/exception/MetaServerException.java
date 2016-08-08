package com.ctrip.xpipe.redis.meta.server.exception;

import com.ctrip.xpipe.redis.core.exception.RedisException;

/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class MetaServerException extends RedisException{

	private static final long serialVersionUID = 1L;

	public MetaServerException(String message) {
		super(message);
	}
	
	public MetaServerException(String msg, Throwable th) {
		super(msg, th);
	}
}
