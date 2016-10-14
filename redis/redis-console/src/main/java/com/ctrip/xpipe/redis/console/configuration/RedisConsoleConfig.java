package com.ctrip.xpipe.redis.console.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleServiceManager;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultMetaServerConsoleServiceManager;
import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;

/**
 * @author shyin
 *
 *         Jul 28, 2016
 */
@Configuration
@EnableAspectJAutoProxy
public class RedisConsoleConfig extends AbstractRedisConfigContext {
	@Bean
	public MetaServerConsoleServiceManager getMetaServerConsoleServiceManager() {
		return new DefaultMetaServerConsoleServiceManager();
	}

	@Bean
	public MetaServerConsoleServiceManagerWrapper getMetaServerConsoleServiceManagerWraper() {
		return new DefaultMetaServerConsoleServiceManagerWrapper();
	}
}
