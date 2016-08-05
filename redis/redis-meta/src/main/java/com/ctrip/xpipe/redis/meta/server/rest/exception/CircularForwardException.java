package com.ctrip.xpipe.redis.meta.server.rest.exception;

import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class CircularForwardException extends MetaRestException{

	private static final long serialVersionUID = 1L;

	public CircularForwardException(ForwardInfo forwardInfo, int currentServerId) {
		super(String.format("forwardinfo:%s, currentServer:%d", forwardInfo, currentServerId));
	}

}
