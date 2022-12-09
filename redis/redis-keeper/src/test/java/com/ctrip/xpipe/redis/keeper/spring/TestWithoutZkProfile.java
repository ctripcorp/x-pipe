package com.ctrip.xpipe.redis.keeper.spring;


import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.keeper.config.*;
import com.ctrip.xpipe.redis.keeper.container.KeeperContainerService;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.monitor.impl.DefaultKeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.ratelimit.CompositeLeakyBucket;
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
@Profile(TestWithoutZkProfile.PROFILE_NO_ZK)
public class TestWithoutZkProfile extends AbstractProfile{
	
	public static final String PROFILE_NO_ZK = "profile_no_zk";
	
	@Bean
	public ZkClient getZkClient(KeeperConfig keeperConfig){
	    String zkNameSpace = System.getProperty("zk.namespace", keeperConfig.getZkNameSpace());
	    String zkAddress = System.getProperty("zk.address", keeperConfig.getZkConnectionString());
		return getZkClient(zkNameSpace, zkAddress);
	}
	
	@Bean
	public KeeperConfig getKeeperConfig(){
		return new TestKeeperConfig(1024, 2, 1024, 2000);
	}
	
	@Bean
	public KeeperContainerConfig getKeeperContainerConfig(){
		return new TestKeeperContainerConfig(AbstractTest.getUserHome() + "/rsd");
	}
	
	@Bean
	public KeepersMonitorManager getKeeperMonitorManager(){
		return new DefaultKeepersMonitorManager();
	}

	@Bean
	public NettySslHandlerFactory getClientSslFactory() {
		return new NettyClientSslHandlerFactory(new DefaultTlsConfig());
	}

	@Bean
	public MetaServerKeeperService getMetaServerKeeperService() {
		return new MetaServerKeeperService() {
			@Override
			public KeeperContainerTokenStatusResponse refreshKeeperContainerTokenStatus(KeeperContainerTokenStatusRequest request) {
				return new KeeperContainerTokenStatusResponse(3);
			}

			@Override
			public KeeperMeta getActiveKeeper(String clusterId, String shardId) {
				return null;
			}
		};
	}

	@Bean
	public KeeperResourceManager getProxyResourceManager(KeeperConfig keeperConfig,
														 MetaServerKeeperService metaServerKeeperService,
														 KeeperContainerService keeperContainerService) {
		CompositeLeakyBucket leakyBucket = getLeakyBucket(keeperConfig, metaServerKeeperService, keeperContainerService);
		return new DefaultKeeperResourceManager(leakyBucket);
	}

	@Bean
	public CompositeLeakyBucket getLeakyBucket(KeeperConfig keeperConfig,
											   MetaServerKeeperService metaServerKeeperService,
											   KeeperContainerService keeperContainerService) {
		return new CompositeLeakyBucket(keeperConfig, metaServerKeeperService, keeperContainerService);
	}
}
