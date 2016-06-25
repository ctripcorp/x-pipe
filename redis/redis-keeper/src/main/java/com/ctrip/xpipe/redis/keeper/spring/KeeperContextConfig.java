package com.ctrip.xpipe.redis.keeper.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.cluster.DefaultLeaderElectorManager;
import com.ctrip.xpipe.lifecycle.SpringComponentLifecycleManager;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.spring.AbstractConfigContext;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;


/**
 * @author marsqing
 *
 *         May 26, 2016 6:23:55 PM
 */
@Configuration
@ComponentScan({"com.ctrip.xpipe.redis.keeper" })
public class KeeperContextConfig extends AbstractConfigContext{
		
	@Bean
	public LeaderElectorManager geElectorManager(KeeperConfig  keeperConfig){
		
		return new DefaultLeaderElectorManager(getZkClient(keeperConfig));
	}

	private ZkClient getZkClient(KeeperConfig keeperConfig) {
		
		DefaultZkClient zkClient = new DefaultZkClient();
		zkClient.setAddress(keeperConfig.getZkConnectionString());
		return zkClient;
	}
	
	
	@Bean
	public SpringComponentLifecycleManager getSpringComponentLifecycleManager(){
		return new SpringComponentLifecycleManager();
	}
	
}
