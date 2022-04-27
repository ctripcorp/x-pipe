package com.ctrip.xpipe.redis.meta.server.spring;

import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.redis.core.route.impl.DefaultRouteChooseStrategyFactory;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperStateController;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.TestZkClient;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * @author wenchao.meng
 *
 * Aug 9, 2016
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class TestProfile extends AbstractProfile implements ApplicationContextAware{
	
	private ApplicationContext applicationContext;
	
	@Bean
	public MetaServerConfig  getMetaServerConfig(){
		return new UnitTestServerConfig();
	}

	@Bean
	public RouteChooseStrategyFactory getRouteChooseStrategyFactory() {
		return new DefaultRouteChooseStrategyFactory();
	}

	
	@Bean
	public ZkClient getZkClient() throws Exception{
		
		TestZkClient zkClient = new TestZkClient();
		zkClient.initialize();
		zkClient.start();
		return zkClient;
	}

	@Bean
	public SpringComponentRegistry getSpringRegistry(){
		return new SpringComponentRegistry((ConfigurableApplicationContext)applicationContext);
	}

	@Bean
	public KeeperStateController getKeeperStateController(){
		return new KeeperStateController() {
			
			@Override
			public void removeKeeper(KeeperTransMeta keeperTransMeta) {
				logger.info("[removeKeeper][test do nothing]{}", keeperTransMeta);
			}
			
			@Override
			public void addKeeper(KeeperTransMeta keeperTransMeta) {
				logger.info("[addKeeper][test do nothing]{}", keeperTransMeta);
			}
		};
	}


	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
