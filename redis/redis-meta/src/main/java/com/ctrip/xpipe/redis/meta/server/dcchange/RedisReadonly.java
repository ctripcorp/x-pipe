package com.ctrip.xpipe.redis.meta.server.dcchange;

import java.util.concurrent.ScheduledExecutorService;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.meta.server.dcchange.exception.RedisReadonlyException;
import com.ctrip.xpipe.redis.meta.server.dcchange.impl.SlaveOfRedisReadOnly;

/**
 * @author wenchao.meng
 *
 * Dec 2, 2016
 */
public interface RedisReadonly {
	
	void makeReadOnly() throws RedisReadonlyException;
	
	void makeWritable() throws RedisReadonlyException;
	
	static RedisReadonly  create(String ip, int port, XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled){
		return new SlaveOfRedisReadOnly(ip, port, keyedObjectPool, scheduled);
	}

}
