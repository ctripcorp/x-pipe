package com.ctrip.xpipe.redis.spring;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;


/**
 * @author marsqing
 *
 *         May 26, 2016 6:23:55 PM
 */
@Configuration
@ComponentScan({ "com.ctrip.xpipe.redis.core", "com.ctrip.xpipe.redis.keeper" })
public class KeeperContextConfig {
	
}
