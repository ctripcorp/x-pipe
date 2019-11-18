package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManagerTest;
import com.ctrip.xpipe.redis.console.alert.message.holder.DefaultAlertEntityHolderTest;
import com.ctrip.xpipe.redis.console.cluster.ConsoleCrossDcServerTest;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfigTest;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfigTest;
import com.ctrip.xpipe.redis.console.controller.api.data.*;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckPrepareRequestTest;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfoTest;
import com.ctrip.xpipe.redis.console.dal.ConcurrentDalTransactionTest;
import com.ctrip.xpipe.redis.console.dal.DalTransactionManagerTest;
import com.ctrip.xpipe.redis.console.dao.*;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.DefaultSiteReliabilityCheckerTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.HealthStatusTest;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.OuterClientServiceProcessorTest;
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
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.clientconfig.CheckClusterTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.ClusterHealthStateTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl.DefaultClusterHealthMonitorManagerTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl.DefaultClusterHealthMonitorTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster.impl.DefaultLeveledEmbededSetTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableCheckTest;
import com.ctrip.xpipe.redis.console.migration.MultiShardMigrationTest;
import com.ctrip.xpipe.redis.console.migration.SingleShardMigrationTest;
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
import com.ctrip.xpipe.redis.console.service.MetaServiceTest;
import com.ctrip.xpipe.redis.console.service.impl.*;
import com.ctrip.xpipe.redis.console.service.meta.impl.AdvancedDcMetaServiceTest;
import com.ctrip.xpipe.redis.console.service.meta.impl.AdvancedDcMetaServiceTestForRoute;
import com.ctrip.xpipe.redis.console.service.meta.impl.ClusterMetaServiceImplTest;
import com.ctrip.xpipe.redis.console.service.migration.impl.DefaultCheckMigrationCommandBuilderTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {

        StateMachineTest.class,
        MigrationStatusTest.class,
        MigrationCheckingStateTest.class,
        MigrationPartialSuccessStateTest.class,
        DefaultMigrationClusterTest.class,
        DefaultMigrationShardTest.class,
        MigrationStatTest.class,
        MigrationInitiatedStateTest.class,
        MigrationPublishStatTest.class,
        SingleShardMigrationTest.class,
        MultiShardMigrationTest.class,
        DefaultShardMigrationResultTest.class,
        MigrationClusterDaoTest.class,
        MigrationEventDaoTest.class,

        AdvancedDcMetaServiceTestForRoute.class,
        AdvancedDcMetaServiceTest.class,
        ClusterMetaServiceImplTest.class,

        MigrationSystemAvailableCheckTest.class,
        DefaultCheckMigrationCommandBuilderTest.class
})
public class AllMigrationTest {
}
