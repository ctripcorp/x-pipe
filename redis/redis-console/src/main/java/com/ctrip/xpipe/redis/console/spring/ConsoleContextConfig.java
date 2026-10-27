package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.sso.LogoutHandler;
import com.ctrip.xpipe.api.sso.UserInfoHolder;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.RelationsService;
import com.ctrip.xpipe.redis.checker.config.impl.CheckConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.ConsoleConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.DataCenterConfigBean;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
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
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.DefaultCurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import com.ctrip.xpipe.redis.core.route.impl.DefaultRouteChooseStrategyFactory;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkClient;
import com.ctrip.xpipe.zk.impl.DefaultZkConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
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
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CONSOLE)
public class ConsoleContextConfig implements XPipeMvcRegistrations {

	public static final String KEEPER_CONTAINER_ZK_CLIENT = "keeperContainerZkClient";

	@Bean
	public DefaultMetaServerConsoleServiceManagerWrapper getMetaServerConsoleServiceManagerWraper(ConsoleConfig config) {
		return new DefaultMetaServerConsoleServiceManagerWrapper(config);
	}

	/**
	 * keepercontainer 地址反查只服务 Console 系模式：CONSOLE_CHECKER 经继承获得本 Bean，
	 * CHECKER 模式的 {@code CheckerContextConfig} 不继承本类，天然不装配、不连该 ZK。
	 */
	@Bean(KEEPER_CONTAINER_ZK_CLIENT)
	@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
	public ZkClient keeperContainerZkClient(ConsoleConfig consoleConfig) {
		DefaultZkConfig zkConfig = new DefaultZkConfig(consoleConfig.getZkConnectionString());
		zkConfig.setZkNameSpace(consoleConfig.getKeeperContainerZkNameSpace());
		ZkClient zkClient = new DefaultZkClient(zkConfig);
		consoleConfig.addListener(zkClient);
		return zkClient;
	}

	/** 同上：仅 Console 系模式装配；Checker 不再 watch ZK 反查容器 Keeper IP。 */
	@Bean
	@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
	public CurrentDcKeeperContainerAddressService keeperContainerAddressService(
			ConsoleConfig consoleConfig,
			@Qualifier(KEEPER_CONTAINER_ZK_CLIENT) ZkClient zkClient) {
		return new DefaultCurrentDcKeeperContainerAddressService(zkClient,
				consoleConfig::getKeeperContainerAddressMaxRemovedPercent,
				consoleConfig::getKeeperContainerAddressMinRemovedHostnames);
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
	public RelationsService relationsService(){
		return new DefaultRelationsService();
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
