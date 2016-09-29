package com.ctrip.xpipe.redis.meta.server.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.job.ConsoleNotifycationTask;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author wenchao.meng
 *
 * Aug 9, 2016
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class Production extends AbstractProfile{
	
	@Bean
	public MetaServerConfig  getMetaServerConfig(){
		return new DefaultMetaServerConfig();
	}
	
	@Bean
	public SpringComponentLifecycleManager getSpringComponentLifecycleManager(){
		return new SpringComponentLifecycleManager();
	}
	
	@Bean
	public ZkClient getZkClient(MetaServerConfig metaServerConfig){
		
		DefaultZkClient zkClient = new DefaultZkClient();
		zkClient.setZkAddress(metaServerConfig.getZkConnectionString());
		return zkClient;
	}
	
	@Bean
	public ConsoleNotifycationTask getConsoleNotifycationTask(){
		return new ConsoleNotifycationTask();
	}
}
