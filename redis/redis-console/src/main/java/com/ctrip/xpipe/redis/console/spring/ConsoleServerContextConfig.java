package com.ctrip.xpipe.redis.console.spring;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import com.ctrip.xpipe.zk.pool.ZkFactory;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
@Configuration
@ComponentScan({"com.ctrip.xpipe.redis.console"})
public class ConsoleServerContextConfig {

	@Bean(name= "zkPool")
	public KeyedObjectPool<String, CuratorFramework>  getZkPool(){
		
		KeyedObjectPool<String, CuratorFramework>  pool = new GenericKeyedObjectPool<String, CuratorFramework>(
				new ZkFactory(new DefaultZkConfig()));
		
		return pool;
	}
}
