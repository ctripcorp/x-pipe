package com.ctrip.xpipe.redis.meta.server.spring;


import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.ApplierStateController;
import com.ctrip.xpipe.redis.meta.server.keeper.applier.manager.DefaultApplierStateController;
import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.job.ConsoleNotifycationTask;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperStateController;
import com.ctrip.xpipe.redis.meta.server.keeper.manager.DefaultKeeperStateController;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcNotifier;
import com.ctrip.xpipe.redis.meta.server.redis.RedisStateManager;
import com.ctrip.xpipe.redis.meta.server.redis.impl.DefaultRedisStateManager;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

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

		ZkClient client = getZkClient(metaServerConfig.getZkNameSpace(), metaServerConfig.getZkConnectionString());
		metaServerConfig.addListener(client);
		return client;
	}
	
	@Bean
	public ConsoleNotifycationTask getConsoleNotifycationTask(){
		return new ConsoleNotifycationTask();
	}
	
	@Bean
	public KeeperStateController getKeeperStateController(){
		
		return new DefaultKeeperStateController();
	}

	@Bean
	public ApplierStateController getApplierStateController(){

		return new DefaultApplierStateController();
	}
	
	@Bean
	public MultiDcNotifier getMultiDcNotifier(){
		return new MultiDcNotifier();
	}
	
	@Bean
	public RedisStateManager getRedisStateManager(){
		return new DefaultRedisStateManager();
	}
}
