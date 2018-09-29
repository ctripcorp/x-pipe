package com.ctrip.xpipe.redis.keeper.spring;


import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.resource.KeeperProxyResourceManager;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.DefaultKeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultKeepersMonitorManager;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

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
		return getZkClient(keeperConfig.getZkNameSpace(), keeperConfig.getZkConnectionString());
	}
	
	@Bean
	public KeeperContainerConfig getKeeperContainerConfig(){
		return new DefaultKeeperContainerConfig();
	}

	@Bean
	public KeeperConfig getKeeperConfig(){
		return new DefaultKeeperConfig();
	}
	
	@Bean
	public KeepersMonitorManager getKeeperMonitorManager(){
		return new DefaultKeepersMonitorManager();
	}

	@Bean
	public ProxyResourceManager getProxyResourceManager() {
		ProxyEndpointManager endpointManager = new DefaultProxyEndpointManager(()->2);
		NextHopAlgorithm algorithm = new NaiveNextHopAlgorithm();
		return new KeeperProxyResourceManager(endpointManager, algorithm);
	}
}
