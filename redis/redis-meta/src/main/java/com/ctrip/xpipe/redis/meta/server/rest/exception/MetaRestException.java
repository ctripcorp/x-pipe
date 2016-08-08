package com.ctrip.xpipe.redis.meta.server.rest.exception;

import com.ctrip.xpipe.redis.meta.server.exception.MetaServerRuntimeException;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class MetaRestException extends MetaServerRuntimeException{

	private static final long serialVersionUID = 1L;

	public MetaRestException(String message) {
		super(message);
	}
	
	public MetaRestException(String msg, Throwable th) {
		super(msg, th);
	}

	

}
