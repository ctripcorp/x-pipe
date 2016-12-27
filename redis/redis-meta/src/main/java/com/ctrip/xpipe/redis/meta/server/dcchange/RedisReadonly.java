package com.ctrip.xpipe.redis.meta.server.dcchange;

/**
 * @author wenchao.meng
 *
 * Dec 2, 2016
 */
public interface RedisReadonly {
	
	void makeReadOnly() throws Exception;
	
	void makeWritable() throws Exception;

}
