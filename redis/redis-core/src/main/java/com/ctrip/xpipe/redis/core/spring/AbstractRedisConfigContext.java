package com.ctrip.xpipe.redis.core.spring;

import org.springframework.context.annotation.Bean;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;

/**
 * @author wenchao.meng
 *
 *         Jun 30, 2016
 */
public class AbstractRedisConfigContext extends AbstractSpringConfigContext {

	@Bean(name = "clientPool")
	public XpipeNettyClientKeyedObjectPool getClientPool() {

		return new XpipeNettyClientKeyedObjectPool();
	}

}
