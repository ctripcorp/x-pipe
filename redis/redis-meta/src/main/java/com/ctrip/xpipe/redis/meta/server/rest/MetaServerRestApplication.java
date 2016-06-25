package com.ctrip.xpipe.redis.meta.server.rest;

import com.ctrip.xpipe.rest.RestApplication;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
public class MetaServerRestApplication extends RestApplication{

	public MetaServerRestApplication() {
		super("com.ctrip.xpipe.redis.meta.server.rest.resource");
	}
}
