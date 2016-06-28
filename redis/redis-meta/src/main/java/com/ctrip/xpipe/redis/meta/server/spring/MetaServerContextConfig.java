package com.ctrip.xpipe.redis.meta.server.spring;

import java.net.InetSocketAddress;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.ctrip.xpipe.pool.XpipeKeyedObjectPool;
import com.ctrip.xpipe.redis.core.client.Client;
import com.ctrip.xpipe.redis.core.client.ClientFactory;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.spring.AbstractConfigContext;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author marsqing
 *
 *         May 26, 2016 6:23:55 PM
 */
@Configuration
@ComponentScan({ "com.ctrip.xpipe.redis.meta.server" })
public class MetaServerContextConfig extends AbstractConfigContext{

	@Bean
	public ZkClient getZkClient(MetaServerConfig metaServerConfig){
		
		DefaultZkClient zkClient = new DefaultZkClient();
		zkClient.setZkAddress(metaServerConfig.getZkConnectionString());
		return zkClient;
	}
	
	@Bean( name = "clientPool")
	public XpipeKeyedObjectPool<InetSocketAddress, Client> getClientPool(){
		
		return new XpipeKeyedObjectPool<>(new ClientFactory());
	}

}
