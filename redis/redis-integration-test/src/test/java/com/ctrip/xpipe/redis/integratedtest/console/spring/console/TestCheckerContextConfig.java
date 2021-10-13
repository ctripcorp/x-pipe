package com.ctrip.xpipe.redis.integratedtest.console.spring.console;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.*;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.cluster.AllCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.cluster.allleader.SentinelMonitorsCheckCrossDc;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.config.impl.DefaultCheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.DefaultPingService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.impl.*;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerMode;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.dao.MigrationShardDao;
import com.ctrip.xpipe.redis.console.healthcheck.meta.DcIgnoredConfigChangeListener;
import com.ctrip.xpipe.redis.console.migration.auto.DefaultBeaconManager;
import com.ctrip.xpipe.redis.console.migration.auto.DefaultMonitorServiceManager;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorServiceManager;
import com.ctrip.xpipe.redis.console.redis.DefaultSentinelManager;
import com.ctrip.xpipe.redis.console.resources.CheckerAllMetaCache;
import com.ctrip.xpipe.redis.console.resources.CheckerMetaCache;
import com.ctrip.xpipe.redis.console.resources.CheckerPersistenceCache;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.impl.AlertEventService;
import com.ctrip.xpipe.redis.console.service.impl.DcClusterShardServiceImpl;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.console.service.meta.impl.BeaconMetaServiceImpl;
import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.integratedtest.console.config.SpringEnvConsoleConfig;
import com.ctrip.xpipe.redis.integratedtest.console.config.TestFoundationService;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;
@Configuration
@ComponentScan(excludeFilters = { @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = {MigrationEventDao.class, MigrationClusterDao.class, MigrationShardDao.class}) },
        basePackages = { "com.ctrip.xpipe.redis.console.dao", "com.ctrip.xpipe.redis.checker" })
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CHECKER)
public class TestCheckerContextConfig {

    @Bean
    public DcClusterShardService dcClusterShardService() {
        return new DcClusterShardServiceImpl();
    }

    @Bean
    public PersistenceCache persistenceCache(CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService,
                                             @Qualifier(value = SCHEDULED_EXECUTOR) ScheduledExecutorService scheduled) {
        return new CheckerPersistenceCache(checkerConfig, checkerConsoleService, scheduled);
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
    public ProxyManager proxyManager(GroupCheckerLeaderElector clusterServer, CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService) {
        return new CheckerProxyManager(clusterServer, checkerConfig, checkerConsoleService);
    }

    @Bean
    public DefaultConsoleConfig consoleConfig() {
        return  new SpringEnvConsoleConfig();
    }

    @Bean
    public CheckerDbConfig checkerDbConfig(PersistenceCache persistenceCache) {
        return new DefaultCheckerDbConfig(persistenceCache);
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
    public GroupCheckerLeaderElector checkerLeaderElector(FoundationService foundationService) {
        return new GroupCheckerLeaderElector(foundationService.getGroupId());
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
    public CheckerCrossMasterDelayManager checkerCrossMasterDelayManager(FoundationService foundationService) {
        return new CheckerCrossMasterDelayManager(foundationService.getDataCenter());
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
                                                   GroupCheckerLeaderElector clusterServer, RedisDelayManager redisDelayManager,
                                                   CrossMasterDelayManager crossMasterDelayManager, PingService pingService,
                                                   ClusterHealthManager clusterHealthManager, HealthStateService healthStateService,
                                                   @Value("${server.port}") int serverPort) {
        return new HealthCheckReporter(healthStateService, checkerConfig, checkerConsoleService, clusterServer, redisDelayManager,
                crossMasterDelayManager, pingService, clusterHealthManager, serverPort);
    }

    @Bean(name = "ALLCHECKER")
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public AllCheckerLeaderElector allCheckerLeaderElector(FoundationService foundationService) {
        return new AllCheckerLeaderElector(foundationService.getDataCenter());
    }
   
    @Bean
    public FoundationService foundationService() {
        return new TestFoundationService();
    }

    @Bean
    public SentinelMonitorsCheckCrossDc sentinelMonitorsCheckCrossDc(PersistenceCache persistenceCache,
                                                                     CheckerConfig config,
                                                                     FoundationService foundationService,
                                                                     CheckerConsoleService service,
                                                                     SentinelManager manager,
                                                                     AlertManager alertManager
    ) {
        return new SentinelMonitorsCheckCrossDc(new CheckerAllMetaCache(config, service), persistenceCache, config, foundationService.getDataCenter(), manager, alertManager);
    }
}
