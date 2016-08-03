package com.ctrip.xpipe.redis.meta.server.rest.exception;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class UnfoundAliveSererException extends MetaRestException{

	private static final long serialVersionUID = 1L;

	public UnfoundAliveSererException(String clusterId, Integer serverId) {
		super(String.format("ServerId:%d unfound(%s)", serverId, clusterId));
	}

}
