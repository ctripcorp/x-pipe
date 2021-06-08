package com.ctrip.xpipe.redis.console.spring;

import com.ctrip.xpipe.api.cluster.ClusterServer;
import com.ctrip.xpipe.redis.checker.*;
import com.ctrip.xpipe.redis.checker.cluster.CheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.config.impl.DefaultCheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.DefaultPingService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.impl.*;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.dao.MigrationShardDao;
import com.ctrip.xpipe.redis.console.migration.auto.DefaultBeaconManager;
import com.ctrip.xpipe.redis.console.migration.auto.DefaultMonitorServiceManager;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorServiceManager;
import com.ctrip.xpipe.redis.console.redis.DefaultSentinelManager;
import com.ctrip.xpipe.redis.console.resources.CheckerMetaCache;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.meta.DcIgnoredConfigChangeListener;
import com.ctrip.xpipe.redis.console.resources.DefaultPersistence;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.impl.AlertEventService;
import com.ctrip.xpipe.redis.console.service.impl.DcClusterShardServiceImpl;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.console.service.meta.impl.BeaconMetaServiceImpl;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.beans.factory.annotation.Autowire;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;

import java.util.concurrent.ExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * @author lishanglin
 * date 2021/3/8
 */
@Configuration
@ComponentScan(excludeFilters = { @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = {MigrationEventDao.class, MigrationClusterDao.class, MigrationShardDao.class}) },
        basePackages = { "com.ctrip.xpipe.redis.console.dao", "com.ctrip.xpipe.redis.checker" })
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CHECKER)
public class CheckerContextConfig {

    @Bean
    public DcClusterShardService dcClusterShardService() {
        return new DcClusterShardServiceImpl();
    }

    @Bean(autowire = Autowire.BY_TYPE)
    public Persistence persistence() {
        return new DefaultPersistence();
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public MetaCache metaCache(CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService) {
        return new CheckerMetaCache(checkerConfig, checkerConsoleService);
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_TEST)
    public MetaCache testMetaCache() {
        return new TestMetaCache();
    }

    @Bean
    public ProxyManager proxyManager(ClusterServer clusterServer, CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService) {
        return new CheckerProxyManager(clusterServer, checkerConfig, checkerConsoleService);
    }

    @Bean
    public DefaultConsoleConfig consoleConfig() {
        return new DefaultConsoleConfig();
    }

    @Bean
    public CheckerDbConfig checkerDbConfig(Persistence persistence, CheckerConfig config) {
        return new DefaultCheckerDbConfig(persistence, config);
    }

    @Bean
    public DcIgnoredConfigChangeListener dcIgnoredConfigChangeListener() {
        return new DcIgnoredConfigChangeListener();
    }

    @Bean
    public MetaServerManager metaServerManager() {
        return new DefaultMetaServerConsoleServiceManagerWrapper();
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public CheckerLeaderElector checkerLeaderElector() {
        return new CheckerLeaderElector();
    }

    @Bean
    public CheckerRedisDelayManager redisDelayManager() {
        return new CheckerRedisDelayManager();
    }

    @Bean
    public CheckerRedisInfoManager redisInfoManager() {
        return new CheckerRedisInfoManager();
    }

    @Bean
    public DefaultPingService pingService() {
        return new DefaultPingService();
    }

    @Bean
    public ClusterHealthManager clusterHealthManager(@Qualifier(GLOBAL_EXECUTOR) ExecutorService executorService) {
        return new CheckerClusterHealthManager(executorService);
    }

    @Bean
    public MonitorServiceManager monitorServiceManager(ConsoleConfig config) {
        return new DefaultMonitorServiceManager(config);
    }

    @Bean
    public BeaconMetaService beaconMetaService(MetaCache metaCache) {
        return new BeaconMetaServiceImpl(metaCache);
    }

    @Bean
    public BeaconManager beaconManager(MonitorServiceManager monitorServiceManager, BeaconMetaService beaconMetaService) {
        return new DefaultBeaconManager(monitorServiceManager, beaconMetaService);
    }

    @Bean
    public CheckerCrossMasterDelayManager checkerCrossMasterDelayManager() {
        return new CheckerCrossMasterDelayManager();
    }

    @Bean
    public RemoteCheckerManager remoteCheckerManager(CheckerConfig checkerConfig) {
        return new DefaultRemoteCheckerManager(checkerConfig);
    }

    @Bean
    public AlertEventService alertEventService() {
        return new AlertEventService();
    }

    @Bean
    @Lazy
    public SentinelManager sentinelManager() {
        return new DefaultSentinelManager();
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public HealthCheckReporter healthCheckReporter(CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService,
                                                   ClusterServer clusterServer, RedisDelayManager redisDelayManager,
                                                   CrossMasterDelayManager crossMasterDelayManager, PingService pingService,
                                                   ClusterHealthManager clusterHealthManager, HealthStateService healthStateService,
                                                   @Value("${server.port}") int serverPort) {
        return new HealthCheckReporter(healthStateService, checkerConfig, checkerConsoleService, clusterServer, redisDelayManager,
                crossMasterDelayManager, pingService, clusterHealthManager, serverPort);
    }

}
