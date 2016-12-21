package com.ctrip.xpipe.redis.meta.server.spring;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.core.metaserver.impl.DefaultMetaServerMultiDcServiceManager;
import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;
import com.ctrip.xpipe.utils.OsUtils;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author marsqing
 *
 *         May 26, 2016 6:23:55 PM
 */
@Configuration
@ComponentScan(basePackages = { "com.ctrip.xpipe.redis.meta.server" })
public class MetaServerContextConfig extends AbstractRedisConfigContext{
	
	public static final String CLIENT_POOL = "clientPool";
	public static final String SCHEDULED_EXECUTOR = "scheduledExecutor";
	public static final int maxScheduledCorePoolSize = 8; 
	
	@Bean(name = CLIENT_POOL)
	public XpipeNettyClientKeyedObjectPool getClientPool() {

		return new XpipeNettyClientKeyedObjectPool();
	}
	
	@Bean(name = SCHEDULED_EXECUTOR)
	public ScheduledExecutorService getScheduledExecutorService(){
		
		int corePoolSize = OsUtils.getCpuCount();
		if(corePoolSize > maxScheduledCorePoolSize){
			corePoolSize = maxScheduledCorePoolSize;
		}
		return MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(corePoolSize));
	}

	@Bean
	public MetaServerMultiDcServiceManager getMetaServerMultiDcServiceManager(){
		
		return new DefaultMetaServerMultiDcServiceManager();
	}
}
