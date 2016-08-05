package com.ctrip.xpipe.redis.meta.server.rest.exception;

import com.ctrip.xpipe.redis.meta.server.exception.RedisMetaServerRuntimeException;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class MetaRestException extends RedisMetaServerRuntimeException{

	private static final long serialVersionUID = 1L;

	public MetaRestException(String message) {
		super(message);
	}
	
	public MetaRestException(String msg, Throwable th) {
		super(msg, th);
	}

	

}
