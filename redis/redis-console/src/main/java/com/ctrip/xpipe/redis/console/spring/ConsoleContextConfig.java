package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.api.sso.LogoutHandler;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.sso.UserAccessFilter;
import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.console.util.MetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.NaiveNextHopAlgorithm;
import com.ctrip.xpipe.redis.core.proxy.netty.ProxyEnabledNettyKeyedPoolClientFactory;
import com.ctrip.xpipe.redis.core.proxy.resource.ConsoleProxyResourceManager;
import com.ctrip.xpipe.redis.core.spring.AbstractRedisConfigContext;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.boot.context.embedded.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.*;

import java.util.concurrent.*;

/**
 * @author shyin
 *
 *         Jul 28, 2016
 */
@Configuration
@EnableAspectJAutoProxy
@ComponentScan(basePackages = {"com.ctrip.xpipe.service.sso"})
@ServletComponentScan("com.ctrip.framework.fireman")
public class ConsoleContextConfig extends AbstractRedisConfigContext {

	public final static String REDIS_COMMAND_EXECUTOR = "redisCommandExecutor";

	public final static String KEYED_NETTY_CLIENT_POOL = "keyedClientPool";

	public final static String PING_DELAY_EXECUTORS = "pingDelayExecutors";

	public final static String PING_DELAY_SCHEDULED = "pingDelayScheduled";

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

	@Bean(name = KEYED_NETTY_CLIENT_POOL)
	public XpipeNettyClientKeyedObjectPool getReqResNettyClientPool() throws Exception {
		XpipeNettyClientKeyedObjectPool keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getKeyedPoolClientFactory());
		LifecycleHelper.initializeIfPossible(keyedObjectPool);
		LifecycleHelper.startIfPossible(keyedObjectPool);
		return keyedObjectPool;
	}

	private ProxyEnabledNettyKeyedPoolClientFactory getKeyedPoolClientFactory() {
		ProxyResourceManager resourceManager = new ConsoleProxyResourceManager(
				new DefaultProxyEndpointManager(()->1), new NaiveNextHopAlgorithm());
		return new ProxyEnabledNettyKeyedPoolClientFactory(resourceManager);
	}

	@Bean(name = PING_DELAY_EXECUTORS)
	public ExecutorService getDelayPingExecturos() {
		return DefaultExecutorFactory.createAllowCoreTimeoutAbortPolicy("RedisHealthCheckInstance-").createExecutorService();
	}

	@Bean(name = PING_DELAY_SCHEDULED)
	public ScheduledExecutorService getDelayPingScheduled() {
		ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(Math.min(OsUtils.getCpuCount(), 4),
				XpipeThreadFactory.create("RedisHealthCheckInstance-Scheduled-"));
		((ScheduledThreadPoolExecutor)scheduled).setRemoveOnCancelPolicy(true);
		((ScheduledThreadPoolExecutor)scheduled).setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
		return scheduled;
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
