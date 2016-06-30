package com.ctrip.xpipe.redis.meta.server.spring;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author marsqing
 *
 *         May 26, 2016 6:23:55 PM
 */
@Configuration
@ComponentScan({ "com.ctrip.xpipe.redis.meta.server" })
public class MetaServerContextConfig extends AbstractRedisConfigContext{

	@Bean
	public ZkClient getZkClient(MetaServerConfig metaServerConfig){
		
		DefaultZkClient zkClient = new DefaultZkClient();
		zkClient.setZkAddress(metaServerConfig.getZkConnectionString());
		return zkClient;
	}
	
}
