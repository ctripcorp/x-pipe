package com.ctrip.xpipe.redis.meta.server.spring;


import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.ctrip.xpipe.lifecycle.SpringComponentRegistry;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.UnitTestServerConfig;
import com.ctrip.xpipe.spring.AbstractProfile;

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
	public SpringComponentRegistry getSpringRegistry(){
		return new SpringComponentRegistry(applicationContext);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
