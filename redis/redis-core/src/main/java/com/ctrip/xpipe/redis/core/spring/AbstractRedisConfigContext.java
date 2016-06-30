package com.ctrip.xpipe.redis.core.spring;

import java.net.InetSocketAddress;

import org.springframework.context.annotation.Bean;

import com.ctrip.xpipe.pool.XpipeKeyedObjectPool;
import com.ctrip.xpipe.redis.core.client.Client;
import com.ctrip.xpipe.redis.core.client.ClientFactory;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;

/**
 * @author wenchao.meng
 *
 * Jun 30, 2016
 */
public class AbstractRedisConfigContext extends AbstractSpringConfigContext{
	
	@Bean( name = "clientPool")
	public XpipeKeyedObjectPool<InetSocketAddress, Client> getClientPool(){
		
		return new XpipeKeyedObjectPool<>(new ClientFactory());
	}

	

}
