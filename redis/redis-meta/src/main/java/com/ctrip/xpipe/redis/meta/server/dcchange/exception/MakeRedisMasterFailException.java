package com.ctrip.xpipe.redis.meta.server.dcchange.exception;

import com.ctrip.xpipe.redis.meta.server.exception.MetaServerRuntimeException;

/**
 * @author wenchao.meng
 *
 * Dec 12, 2016
 */
public class MakeRedisMasterFailException extends MetaServerRuntimeException{

	private static final long serialVersionUID = 1L;

	public MakeRedisMasterFailException(String message, Throwable th) {
		super(message, th);
	}
	
	

}
