package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.api.sso.LogoutHandler;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.sso.UserAccessFilter;
import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.boot.context.embedded.FilterRegistrationBean;
import org.springframework.context.annotation.*;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author shyin
 *
 *         Jul 28, 2016
 */
@Configuration
@EnableAspectJAutoProxy
@ComponentScan(basePackages = {"com.ctrip.xpipe.service.sso"})
public class ConsoleContextConfig extends AbstractRedisConfigContext {

	public final static String REDIS_COMMAND_EXECUTOR = "redisCommandExecutor";

	public final static String REQUEST_RESPONSE_NETTY_CLIENT_POOL = "reqResNettyClientPool";

	public final static String SUBSCRIBE_NETTY_CLIENT_POOL = "subscribeNettyClientPool";

	private ProxyResourceManager resourceManager;

	@Bean(name = REDIS_COMMAND_EXECUTOR)
	public ScheduledExecutorService getRedisCommandExecutor() {
		int corePoolSize = OsUtils.getCpuCount();
		if (corePoolSize > maxScheduledCorePoolSize) {
			corePoolSize = maxScheduledCorePoolSize;
		}
		return MoreExecutors.getExitingScheduledExecutorService(
				new ScheduledThreadPoolExecutor(corePoolSize, XpipeThreadFactory.create(REDIS_COMMAND_EXECUTOR)),
				THREAD_POOL_TIME_OUT, TimeUnit.SECONDS
		);
	}

	@Bean
	public MetaServerConsoleServiceManagerWrapper getMetaServerConsoleServiceManagerWraper() {
		return new DefaultMetaServerConsoleServiceManagerWrapper();
	}

	@Bean(name = REQUEST_RESPONSE_NETTY_CLIENT_POOL)
	public XpipeNettyClientKeyedObjectPool getReqResNettyClientPool() throws Exception {
		XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(OsUtils.getCpuCount());
		LifecycleHelper.initializeIfPossible(keyedObjectPool);
		LifecycleHelper.startIfPossible(keyedObjectPool);
		return keyedObjectPool;
	}

	@Bean(name = SUBSCRIBE_NETTY_CLIENT_POOL)
	public XpipeNettyClientKeyedObjectPool getSubscribeNettyClientPool() throws Exception {
		XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(OsUtils.getCpuCount());
		LifecycleHelper.initializeIfPossible(keyedObjectPool);
		LifecycleHelper.startIfPossible(keyedObjectPool);
		return keyedObjectPool;
	}

	@Bean
	public ConsoleConfig consoleConfig(){
		return new DefaultConsoleConfig();
	}
	@Bean
	public UserInfoHolder userInfoHolder(){
		return UserInfoHolder.DEFAULT;
	}

	@Bean
	public LogoutHandler logoutHandler(){
		return LogoutHandler.DEFAULT;
	}

	@Bean
	@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
	public FilterRegistrationBean userAccessFilter(ConsoleConfig consoleConfig) {

		FilterRegistrationBean userAccessFilter = new FilterRegistrationBean();

		userAccessFilter.setFilter(new UserAccessFilter(UserInfoHolder.DEFAULT, consoleConfig));
		userAccessFilter.addUrlPatterns("/*");

		return userAccessFilter;
	}
}
