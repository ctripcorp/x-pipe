package com.ctrip.xpipe.redis.integratedtest.console.spring.console;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.*;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.cluster.AllCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.config.impl.DefaultCheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.HealthStateService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.DefaultPingService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.checker.healthcheck.allleader.SentinelMonitorsCheckCrossDc;
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
import com.ctrip.xpipe.redis.console.migration.auto.DefaultMonitorManager;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.console.redis.DefaultSentinelManager;
import com.ctrip.xpipe.redis.console.resources.CheckerAllMetaCache;
import com.ctrip.xpipe.redis.console.resources.CheckerCurrentDcAllMeta;
import com.ctrip.xpipe.redis.console.resources.CheckerMetaCache;
import com.ctrip.xpipe.redis.console.resources.CheckerPersistenceCache;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.impl.DcClusterShardServiceImpl;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.console.service.meta.impl.BeaconMetaServiceImpl;
import com.ctrip.xpipe.redis.console.util.DefaultMetaServerConsoleServiceManagerWrapper;
import com.ctrip.xpipe.redis.core.meta.CurrentDcAllMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.integratedtest.console.config.SpringEnvConsoleConfig;
import com.ctrip.xpipe.redis.integratedtest.console.config.TestFoundationService;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;

import java.util.concurrent.ExecutorService;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
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
    public PersistenceCache persistenceCache(CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService) {
        return new CheckerPersistenceCache(checkerConfig, checkerConsoleService);
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public CurrentDcAllMeta currentDcAllMeta() {
        return new CheckerCurrentDcAllMeta();
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public MetaCache metaCache(CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService) {
        return new CheckerMetaCache(checkerConfig, checkerConsoleService);
    }

    @Bean
    public CheckerAllMetaCache checkerAllMetaCache() {
        return new CheckerAllMetaCache();
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_TEST)
    public MetaCache testMetaCache() {
        return new TestMetaCache();
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_TEST)
    public CurrentDcAllMeta testCurrentDcAllMeta() {
        return new TestCurrentDcAllMetaCache();
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
    public MonitorManager monitorServiceManager(MetaCache metaCache, ConsoleConfig config) {
        return new DefaultMonitorManager(metaCache, config);
    }

    @Bean
    public BeaconMetaService beaconMetaService(MetaCache metaCache, ConsoleConfig config) {
        return new BeaconMetaServiceImpl(metaCache, config);
    }

    @Bean
    public BeaconManager beaconManager(MonitorManager monitorManager, BeaconMetaService beaconMetaService, CheckerConfig config) {
        return new DefaultBeaconManager(monitorManager, beaconMetaService, config);
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
    @Lazy
    public SentinelManager sentinelManager() {
        return new DefaultSentinelManager();
    }

    @Bean
    @Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
    public HealthCheckReporter healthCheckReporter(CheckerConfig checkerConfig, CheckerConsoleService checkerConsoleService,
                                                   GroupCheckerLeaderElector clusterServer, AllCheckerLeaderElector allCheckerLeaderElector, RedisDelayManager redisDelayManager,
                                                   CrossMasterDelayManager crossMasterDelayManager, PingService pingService,
                                                   ClusterHealthManager clusterHealthManager, HealthStateService healthStateService,
                                                   @Value("${server.port}") int serverPort) {
        return new HealthCheckReporter(healthStateService, checkerConfig, checkerConsoleService, clusterServer, allCheckerLeaderElector, redisDelayManager,
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
    public SentinelMonitorsCheckCrossDc sentinelMonitorsCheckCrossDc(CheckerAllMetaCache metaCache,PersistenceCache persistenceCache,
                                                                     CheckerConfig config,
                                                                     FoundationService foundationService,
                                                                     SentinelManager manager,
                                                                     AlertManager alertManager
    ) {
        return new SentinelMonitorsCheckCrossDc(metaCache, persistenceCache, config, foundationService.getDataCenter(), manager, alertManager);
    }
}
