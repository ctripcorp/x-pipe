package com.ctrip.xpipe.redis.keeper.spring;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.resource.KeeperProxyResourceManager;
import com.ctrip.xpipe.redis.keeper.config.*;
import com.ctrip.xpipe.redis.keeper.monitor.KeepersMonitorManager;
import com.ctrip.xpipe.redis.keeper.monitor.impl.NoneKeepersMonitorManager;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkTestServer;
import com.ctrip.xpipe.zk.impl.TestZkClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * @author wenchao.meng
 *
 * Aug 22, 2016
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class TestProfile extends AbstractProfile{
	
	@Bean
	public ZkClient getZkClient(ZkTestServer zkTestServer) throws Exception{
		
		TestZkClient zkClient = new TestZkClient();
		zkClient.setZkAddress(String.format("%s:%d", "localhost", zkTestServer.getZkPort()));
		zkClient.initialize();
		zkClient.start();
		logger.info("[getZkClient][test]");
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
	
	@Bean
	public ZkTestServer getZkTestServer() throws Exception{
		
		int zkPort = AbstractTest.randomPort();
		ZkTestServer zkTestServer = new ZkTestServer(zkPort);
		zkTestServer.initialize();
		zkTestServer.start();
		return zkTestServer;
	}
	
	@Bean
	public KeepersMonitorManager getKeeperMonitorManager(){
		return new NoneKeepersMonitorManager();
	}

	@Bean
	public NettySslHandlerFactory getClientSslFactory() {
		return new NettyClientSslHandlerFactory(new DefaultTlsConfig());
	}

	@Bean
	public ProxyResourceManager getProxyResourceManager() {
		ProxyEndpointManager endpointManager = new DefaultProxyEndpointManager(()->60);
		NextHopAlgorithm algorithm = new NaiveNextHopAlgorithm();
		return new KeeperProxyResourceManager(endpointManager, algorithm);
	}
	
}
