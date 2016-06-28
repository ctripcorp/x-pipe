package com.ctrip.xpipe.redis.console.spring;


import org.apache.curator.framework.CuratorFramework;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.ctrip.xpipe.pool.XpipeKeyedObjectPool;
import com.ctrip.xpipe.spring.AbstractConfigContext;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import com.ctrip.xpipe.zk.pool.ZkFactory;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
@Configuration
@ComponentScan({"com.ctrip.xpipe.redis.console"})
public class ConsoleServerContextConfig extends AbstractConfigContext{

	@Bean(name= "zkPool")
	public XpipeKeyedObjectPool<String, CuratorFramework>  getZkPool(){
		
		XpipeKeyedObjectPool<String, CuratorFramework> pool = new XpipeKeyedObjectPool<>(new ZkFactory(new DefaultZkConfig()));
		return pool;
	}
}
