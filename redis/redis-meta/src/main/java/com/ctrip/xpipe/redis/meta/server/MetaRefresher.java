package com.ctrip.xpipe.redis.meta.server;

/**
 * @author wenchao.meng
 *
 * Jun 25, 2016
 */
public interface MetaRefresher {
	
	void update() throws Exception;
}
