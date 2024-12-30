package com.ctrip.xpipe.redis.keeper.spring;


import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultMetaServerKeeperService;
import com.ctrip.xpipe.redis.keeper.config.*;
import com.ctrip.xpipe.redis.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultKeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.ratelimit.CompositeLeakyBucket;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.LeakyBucket;
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
		ZkClient client = getZkClient(keeperConfig.getZkNameSpace(), keeperConfig.getZkConnectionString());
		keeperConfig.addListener(client);
		return client;
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
	public KeeperCommonConfig getKeeperCommonConfig() {
		return new DefaultKeeperCommonConfig();
	}
	
	@Bean
	public KeepersMonitorManager getKeeperMonitorManager(){
		return new DefaultKeepersMonitorManager();
	}

	@Bean
	public MetaServerKeeperService getMetaServerKeeperService(KeeperConfig keeperConfig) {
		return new DefaultMetaServerKeeperService(()->keeperConfig.getMetaServerAddress());
	}

	@Bean
	public KeeperResourceManager getKeeperResourceManager(LeakyBucket leakyBucket) {

		return new DefaultKeeperResourceManager(leakyBucket);
	}

	@Bean(initMethod = "start", destroyMethod = "stop")
	public CompositeLeakyBucket getLeakyBucket(KeeperConfig keeperConfig,
                                               MetaServerKeeperService metaServerKeeperService,
                                               KeeperContainerService keeperContainerService) {
		return new CompositeLeakyBucket(keeperConfig, metaServerKeeperService, keeperContainerService);
	}
}
