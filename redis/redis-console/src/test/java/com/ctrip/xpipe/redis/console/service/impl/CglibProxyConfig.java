package com.ctrip.xpipe.redis.console.service.impl;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * @author: cchen6
 * 2020/10/15
 */

// only for ShardServiceImplTest
@Configuration
@EnableAspectJAutoProxy(proxyTargetClass = true)
public class CglibProxyConfig {

}
