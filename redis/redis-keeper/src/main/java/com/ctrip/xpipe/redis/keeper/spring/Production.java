package com.ctrip.xpipe.redis.keeper.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;

/**
 * @author wenchao.meng
 *
 * Aug 22, 2016
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class Production extends AbstractProfile{

	@Bean
	public ZkClient getZkClient(KeeperConfig keeperConfig) {
		
		DefaultZkClient zkClient = new DefaultZkClient();
		zkClient.setZkAddress(keeperConfig.getZkConnectionString());
		return zkClient;
	}
	
	@Bean
	public KeeperContainerConfig getKeeperContainerConfig(){
		return new DefaultKeeperContainerConfig();
	}

	@Bean
	public KeeperConfig getKeeperConfig(){
		return new DefaultKeeperConfig();
	}

}
