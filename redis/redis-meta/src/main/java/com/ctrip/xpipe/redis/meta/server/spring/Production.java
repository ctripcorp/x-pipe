package com.ctrip.xpipe.redis.meta.server.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import com.ctrip.xpipe.redis.meta.server.config.DefaultMetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;

/**
 * @author wenchao.meng
 *
 * Aug 9, 2016
 */
@Configuration
@Profile("production")
public class Production {
	
	@Bean
	public MetaServerConfig  getMetaServerConfig(){
		return new DefaultMetaServerConfig();
	}

}
