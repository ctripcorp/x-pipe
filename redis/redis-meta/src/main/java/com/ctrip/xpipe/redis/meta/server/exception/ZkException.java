package com.ctrip.xpipe.redis.meta.server.exception;

/**
 * @author wenchao.meng
 *
 * Aug 6, 2016
 */
public class ZkException extends MetaServerRuntimeException{

	private static final long serialVersionUID = 1L;

	public ZkException(String message, Exception e) {
		super(message, e);
	}

}
