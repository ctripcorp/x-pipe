package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;

/**
 * @author wenchao.meng
 *
 * Jun 27, 2016
 */
public class MetaException extends RedisRuntimeException{

	private static final long serialVersionUID = 1L;

	public MetaException(String message) {
		super(message);
	}
	
	

}
