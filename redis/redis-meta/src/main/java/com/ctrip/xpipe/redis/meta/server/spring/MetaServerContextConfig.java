package com.ctrip.xpipe.redis.meta.server.spring;


import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;

/**
 * @author marsqing
 *
 *         May 26, 2016 6:23:55 PM
 */
@Configuration
@ComponentScan(basePackages = { "com.ctrip.xpipe.redis.meta.server" })
public class MetaServerContextConfig extends AbstractRedisConfigContext{
	
}
