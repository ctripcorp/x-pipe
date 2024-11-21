package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.sso.LogoutHandler;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.checker.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.config.impl.CheckConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.ConsoleConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.DataCenterConfigBean;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.impl.DefaultKeeperContainerService;
import com.ctrip.xpipe.redis.checker.impl.TestMetaCache;
import com.ctrip.xpipe.redis.checker.resource.DefaultCheckerConsoleService;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.console.cluster.ConsoleCrossDcServer;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfig;
import com.ctrip.xpipe.redis.console.dao.ClusterDao;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.dao.RedisDao;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthMonitorManager;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl.DefaultClusterHealthMonitorManager;
import com.ctrip.xpipe.redis.console.keeper.KeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.keeper.impl.DefaultKeeperContainerUsedInfoAnalyzer;
import com.ctrip.xpipe.redis.console.keeper.impl.KeeperContainerMigrationAnalyzer;
import com.ctrip.xpipe.redis.console.notifier.cluster.ClusterTypeUpdateEventFactory;
import com.ctrip.xpipe.redis.console.resources.*;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.impl.*;
import com.ctrip.xpipe.redis.console.sso.UserAccessFilter;
import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.redis.core.route.impl.DefaultRouteChooseStrategyFactory;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.annotation.*;


/**
 * @author shyin
 *
 *         Jul 28, 2016
 */
@Configuration
@EnableAspectJAutoProxy
@ComponentScan(basePackages = {"com.ctrip.xpipe.service",
		"com.ctrip.xpipe.redis.console",
		"com.ctrip.xpipe.redis.checker.alert"},
		excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "com\\.ctrip\\.xpipe\\.service\\.ignite\\.DalIgniteValidate")
)
@ServletComponentScan("com.ctrip.framework.fireman")
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CONSOLE)
public class ConsoleContextConfig implements XPipeMvcRegistrations {

	@Bean
	public DefaultMetaServerConsoleServiceManagerWrapper getMetaServerConsoleServiceManagerWraper(ConsoleConfig config) {
		return new DefaultMetaServerConsoleServiceManagerWrapper(config);
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

	@Bean
	@Lazy
	@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
	public MetaCache metaCache(ConsolePortalService consolePortalService, ConsoleConfig config) {
		if(config.disableDb()) {
			return new ConsoleMetaCacheWithoutDB(consolePortalService, config);
		}
		return new DefaultMetaCache();
	}

	@Bean
	public RouteChooseStrategyFactory getRouteChooseStrategyFactory() {
		return new DefaultRouteChooseStrategyFactory();
	}

	@Bean(name = "metaCache")
	@Lazy
	@Profile(AbstractProfile.PROFILE_NAME_TEST)
	public MetaCache testMetaCache() {
		return new TestMetaCache();
	}

	@Bean
	public ConsoleConfig consoleConfig(FoundationService foundationService) {
		return new DefaultConsoleConfig(
				new CheckConfigBean(foundationService),
				new ConsoleConfigBean(foundationService),
				new DataCenterConfigBean(),
				new CommonConfigBean());
	}

	@Bean
	public ConsoleDbConfig consoleDbConfig() {
		return new DefaultConsoleDbConfig();
	}

	@Bean
	public DcRelationsService dcRelationsService(){
		return new DefaultDcRelationsService();
	}

	@Bean
	@DependsOn("metaCache")
	@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
	public ConsoleLeaderElector consoleLeaderElector() {
		return new ConsoleLeaderElector();
	}

	@Bean
	public PingService pingService() {
		return new ConsoleCachedPingService();
	}

	@Bean
	public KeeperContainerUsedInfoAnalyzer KeeperContainerUsedInfoAnalyzer(ConsoleConfig config,
																		   KeeperContainerMigrationAnalyzer migrationAnalyzer){
		return new DefaultKeeperContainerUsedInfoAnalyzer(config, migrationAnalyzer);
	}

	@Bean
	public RedisInfoService redisInfoService() {
	    return new ConsoleRedisInfoService();
	}

	@Lazy
	@Bean
	@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
	public ClusterHealthMonitorManager clusterHealthManager() {
		return new DefaultClusterHealthMonitorManager();
	}

	@Bean
	public DefaultCrossMasterDelayService defaultCrossMasterDelayService(FoundationService foundationService) {
		return new DefaultCrossMasterDelayService(foundationService.getDataCenter());
	}

	@Bean
	public PersistenceCache persistenceCache3(ConsoleConfig config,
											  AlertEventService alertEventService,
											  ConfigDao configDao,
											  DcClusterShardService dcClusterShardService,
											  RedisDao redisDao,
											  ClusterDao clusterDao
											  ) {
		if(config.disableDb()) {
			return new PersistenceCacheWithoutDB(config, new DefaultCheckerConsoleService());
		}
		return new DefaultPersistenceCache(
				config, 
				alertEventService,
				configDao,
				dcClusterShardService,
				redisDao,
				clusterDao);
	}
	
	@Bean
	public FoundationService foundationService() {
		return FoundationService.DEFAULT;
	}

	@Bean
	public KeeperContainerCheckerService keeperContainerService() {
		return new DefaultKeeperContainerService();
	}

	@Bean
	@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
	public MetaSynchronizer metaSynchronizer(ConsoleConfig consoleConfig, ConsoleLeaderElector leaderElector,
											 MetaCache metaCache, RedisService redisService, ShardService shardService,
											 ClusterService clusterService, DcService dcService, OrganizationService organizationService,
											 SentinelBalanceService sentinelBalanceService, ClusterTypeUpdateEventFactory clusterTypeUpdateEventFactory,
											 FoundationService foundationService, ConsoleCrossDcServer consoleCrossDcServer) {
		return new MetaSynchronizer(consoleConfig, leaderElector, metaCache, redisService, shardService,clusterService, dcService,
				organizationService, sentinelBalanceService, clusterTypeUpdateEventFactory,
				foundationService, consoleCrossDcServer);
	}

}
