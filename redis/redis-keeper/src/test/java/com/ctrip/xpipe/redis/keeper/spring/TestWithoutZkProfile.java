package com.ctrip.xpipe.redis.keeper.spring;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperContainerConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.TestZkClient;

/**
 * @author wenchao.meng
 *
 * Aug 22, 2016
 */
@Configuration
@Profile(TestWithoutZkProfile.PROFILE_NO_ZK)
public class TestWithoutZkProfile extends AbstractProfile{
	
	public static final String PROFILE_NO_ZK = "profile_no_zk";
	
	@Bean
	public ZkClient getZkClient(){
		
		TestZkClient zkClient = new TestZkClient();
		return zkClient;
	}
	
	@Bean
	public KeeperConfig getKeeperConfig(){
		return new TestKeeperConfig(1024, 2, 1024, 2000);
	}
	
	@Bean
	public KeeperContainerConfig getKeeperContainerConfig(){
		return new TestKeeperContainerConfig(AbstractTest.getUserHome() + "/rsd");
	}
}
