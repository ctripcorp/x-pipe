package com.ctrip.xpipe.redis.console;


import com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManagerTest;
import com.ctrip.xpipe.redis.console.alert.message.holder.DefaultAlertEntityHolderTest;
import com.ctrip.xpipe.redis.console.cluster.ConsoleCrossDcServerTest;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfigTest;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfigTest;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManagerTest;
import com.ctrip.xpipe.redis.console.controller.api.ChangeConfigTest;
import com.ctrip.xpipe.redis.console.controller.api.HealthControllerTest;
import com.ctrip.xpipe.redis.console.controller.api.data.*;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckPrepareRequestTest;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfoTest;
import com.ctrip.xpipe.redis.console.dal.ConcurrentDalTransactionTest;
import com.ctrip.xpipe.redis.console.dal.DalTransactionManagerTest;
import com.ctrip.xpipe.redis.console.dao.*;
import com.ctrip.xpipe.redis.console.election.CrossDcLeaderElectionActionTest;
import com.ctrip.xpipe.redis.console.healthcheck.NettyKeyedPoolClientFactoryTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayServiceTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.CrossMasterDelayServiceTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayActionTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.MultiMasterDelayActionControllerTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.MultiMasterDelayListenerTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.*;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster.DefaultRedisMasterActionListenerTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster.RedisMasterControllerTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.conflic.ConflictCheckActionFactoryTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.conflic.ConflictCheckActionTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.conflic.ConflictMetricListenerTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.expiresize.ExpireSizeCheckActionTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.expiresize.ExpireSizeMetricListenerTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.tombstonesize.TombstoneSizeCheckActionTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.tombstonesize.TombstoneSizeMetricListenerTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.collector.CurrentDcSentinelHelloAggregationCollectorTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.collector.CurrentDcSentinelHelloCollectorTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.collector.DefaultSentinelHelloCollectorTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.collector.SentinelCollector4KeeperTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.controller.CurrentDcSentinelCheckControllerTest;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultHealthCheckerMockTest;
import com.ctrip.xpipe.redis.console.healthcheck.meta.DefaultDcMetaChangeManagerTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.clientconfig.CheckClusterTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.handler.TestAbstractHealthEventHandlerTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisconf.diskless.DiskLessReplCheckActionTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisconf.version.VersionCheckActionFactoryTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redisconf.version.VersionCheckActionTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster.RedisMasterCheckActionFactoryTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster.RedisMasterCheckActionTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.sentinel.*;
import com.ctrip.xpipe.redis.console.healthcheck.factory.DefaultHealthCheckEndpointFactoryTest;
import com.ctrip.xpipe.redis.console.healthcheck.factory.DefaultRedisHealthCheckInstanceFactoryTest;
import com.ctrip.xpipe.redis.console.healthcheck.factory.HealthCheckEndpointFactoryTest;
import com.ctrip.xpipe.redis.console.healthcheck.meta.DcIgnoredConfigListenerTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthStateTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl.DefaultClusterHealthMonitorManagerTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl.DefaultClusterHealthMonitorTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl.DefaultLeveledEmbededSetTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables.DBVariablesCheckTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.dbvariables.checker.VariablesCheckerTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.metacache.MetaCacheCheckTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableCheckTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.monitor.DefaultSentinelMonitorsCheckTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.sentinelconfig.SentinelConfigCheckTest;
import com.ctrip.xpipe.redis.console.migration.SingleShardMigrationTest;
import com.ctrip.xpipe.redis.console.migration.manager.DefaultMigrationEventManagerTest;
import com.ctrip.xpipe.redis.console.migration.model.DefaultMigrationClusterTest;
import com.ctrip.xpipe.redis.console.migration.model.DefaultMigrationShardTest;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultShardMigrationResultTest;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatTest;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatusTest;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationCheckingStateTest;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationInitiatedStateTest;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPartialSuccessStateTest;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPublishStatTest;
import com.ctrip.xpipe.redis.console.migration.status.migration.statemachine.StateMachineTest;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTest;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifierTest;
import com.ctrip.xpipe.redis.console.notifier.MetaNotifyTaskTest;
import com.ctrip.xpipe.redis.console.proxy.ProxyPingRecorderTest;
import com.ctrip.xpipe.redis.console.proxy.impl.*;
import com.ctrip.xpipe.redis.console.resources.DefaultMetaCacheTest;
import com.ctrip.xpipe.redis.console.service.MetaServiceTest;
import com.ctrip.xpipe.redis.console.service.impl.*;
import com.ctrip.xpipe.redis.console.service.meta.impl.AdvancedDcMetaServiceTest;
import com.ctrip.xpipe.redis.console.service.meta.impl.AdvancedDcMetaServiceTestForRoute;
import com.ctrip.xpipe.redis.console.service.meta.impl.ClusterMetaServiceImplTest;
import com.ctrip.xpipe.redis.console.service.meta.impl.ShardMetaServiceImplTest;
import com.ctrip.xpipe.redis.console.service.migration.impl.DefaultCheckMigrationCommandBuilderTest;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationServiceImplPaginationTest;
import com.ctrip.xpipe.redis.console.service.vo.DcMetaBuilderTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * @author wenchao.meng
 * <p>
 * Jun 23, 2016
 */
@RunWith(Suite.class)
@SuiteClasses(value = {
        SentinelHelloTest.class,
        DefaultSentinelHelloCollectorTest.class,
        SentinelCollector4KeeperTest.class,
        ConsoleCrossDcServerTest.class,

        ClusterServiceImplTest.class,
        RedisServiceImplTest.class,
        KeeperContainerServiceImplTest.class,
        ShardServiceImplTest.class,
        ShardServiceImplTest2.class,
        SentinelServiceImplTest.class,
        ClusterMetaServiceImplTest.class,
        DcServiceImplTest.class,

        StateMachineTest.class,
        MigrationStatusTest.class,
        ConcurrentDalTransactionTest.class,
        DalTransactionManagerTest.class,
        ClusterMetaModifiedNotifierTest.class,
        MetaServiceTest.class,
        ClusterMetaModifiedNotifierTest.class,
        MetaNotifyTaskTest.class,
        MigrationCheckingStateTest.class,
        MigrationPartialSuccessStateTest.class,
        DefaultMigrationClusterTest.class,
        DefaultMigrationShardTest.class,
        MigrationStatTest.class,
        MigrationInitiatedStateTest.class,
        MigrationPublishStatTest.class,
        SingleShardMigrationTest.class,
//        MultiShardMigrationTest.class,
        DefaultShardMigrationResultTest.class,
        ClusterCreateInfoTest.class,
        ClusterServiceImplTest3.class,

        CheckPrepareRequestTest.class,
        ConfigDaoTest.class,
        MigrationClusterDaoTest.class,
        MigrationEventDaoTest.class,
        DefaultConsoleDbConfigTest.class,
        DefaultConsoleConfigTest.class,
        RedisDaoTest.class,
        CheckClusterTest.class,

        KeeperUpdateControllerTest.class,
        MetaUpdateTest.class,
        MetaUpdateTest2.class,
        MetaUpdateTest3.class,
        MetaUpdateTest4.class,
        AlertPolicyManagerTest.class,

        ProxyDaoTest.class,
        RouteDaoTest.class,
        RouteServiceImplTest.class,
        RouteServiceImplTest.class,
        ProxyServiceImplTest.class,

        AdvancedDcMetaServiceTestForRoute.class,
        AdvancedDcMetaServiceTest.class,
        ClusterMetaServiceImplTest.class,

        HealthCheckEndpointFactoryTest.class,
        DefaultHealthCheckEndpointFactoryTest.class,
        DefaultRedisHealthCheckInstanceFactoryTest.class,
        DcIgnoredConfigListenerTest.class,

        DefaultSiteReliabilityCheckerTest.class,
        HealthStatusTest.class,
        OuterClientServiceProcessorTest.class,
        TestAbstractHealthEventHandlerTest.class,
        VersionCheckActionTest.class,
        DiskLessReplCheckActionTest.class,
        RedisMasterCheckActionTest.class,
        SentinelHelloCheckActionFactoryTest.class,
        RedisMasterCheckActionFactoryTest.class,
        SentinelHelloCheckActionTest.class,
        VersionCheckActionFactoryTest.class,
        DefaultProxyMonitorCollectorManagerTest.class,
        DefaultProxyChainAnalyzerTest.class,
        ProxyPingRecorderTest.class,
        DcClusterShardTest.class,
        TestForAbstractMultiValueTunnelSocketStatsAnalyzer.class,
        TestForAbstractNormalKeyValueTunnelSocketStatsAnalyzer.class,
        DefaultTunnelSocketStatsAnalyzerManagerTest.class,
        TunnelSocketStatsAnalyzersTest.class,
        ClusterHealthStateTest.class,
        DefaultLeveledEmbededSetTest.class,
        DefaultClusterHealthMonitorTest.class,
        DefaultClusterHealthMonitorManagerTest.class,
        MigrationSystemAvailableCheckTest.class,
        DefaultCheckMigrationCommandBuilderTest.class,
        DefaultAlertEntityHolderTest.class,
        SentinelUpdateControllerTest.class,
        DefaultDcMetaChangeManagerTest.class,
        MetaCacheCheckTest.class,
        RouteHealthEventProcessorTest.class,
        DefaultMigrationEventManagerTest.class,
        MigrationServiceImplPaginationTest.class,
        SentinelConfigCheckTest.class,
        ClusterServiceImplTest4.class,
        DelayActionTest.class,
        DelayServiceTest.class,
        SentinelHelloActionDowngradeTest.class,
        ShardMetaServiceImplTest.class,
        CrossDcLeaderElectionActionTest.class,
        ShardDaoTest.class,
        SentinelLeakyBucketTest.class,
        VariablesCheckerTest.class,
        DBVariablesCheckTest.class,
        DefaultSentinelMonitorsCheckTest.class,
        DcMetaBuilderTest.class,
        DcClusterServiceImplTest.class,
        MultiMasterDelayListenerTest.class,
        CrossMasterDelayServiceTest.class,
        MultiMasterDelayActionControllerTest.class,
        CurrentDcSentinelHelloAggregationCollectorTest.class,
        CurrentDcSentinelHelloCollectorTest.class,
        CurrentDcSentinelCheckControllerTest.class,
        RedisMasterControllerTest.class,
        CurrentDcDelayPingActionCollectorTest.class,
        HealthControllerTest.class,
        DefaultHealthCheckerMockTest.class,
        ConflictMetricListenerTest.class,
        ConflictCheckActionTest.class,
        ConflictCheckActionFactoryTest.class,
        ExpireSizeMetricListenerTest.class,
        ExpireSizeCheckActionTest.class,
        TombstoneSizeCheckActionTest.class,
        TombstoneSizeMetricListenerTest.class,
        DefaultRedisMasterActionListenerTest.class,
        DefaultMetaCacheTest.class,
        ConsoleServiceManagerTest.class,
        ChangeConfigTest.class,
        NettyKeyedPoolClientFactoryTest.class,

})
public class AllTests {

}
