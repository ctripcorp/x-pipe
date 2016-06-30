package com.ctrip.xpipe.redis.keeper.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.cluster.DefaultLeaderElectorManager;
import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;


/**
 * @author marsqing
 *
 *         May 26, 2016 6:23:55 PM
 */
@Configuration
@ComponentScan({"com.ctrip.xpipe.redis.keeper" })
public class KeeperContextConfig extends AbstractRedisConfigContext{
		
	@Bean
	public LeaderElectorManager geElectorManager(KeeperConfig  keeperConfig){
		
		return new DefaultLeaderElectorManager(getZkClient(keeperConfig));
	}

	private ZkClient getZkClient(KeeperConfig keeperConfig) {
		
		DefaultZkClient zkClient = new DefaultZkClient();
		zkClient.setZkAddress(keeperConfig.getZkConnectionString());
		return zkClient;
	}
	
}
