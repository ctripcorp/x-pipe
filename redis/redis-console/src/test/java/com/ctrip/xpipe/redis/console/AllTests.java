package com.ctrip.xpipe.redis.console;


import com.ctrip.xpipe.redis.console.alert.manager.AlertPolicyManagerTest;
import com.ctrip.xpipe.redis.console.cluster.ConsoleCrossDcServerTest;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfigTest;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfigTest;
import com.ctrip.xpipe.redis.console.controller.api.data.KeeperUpdateControllerTest;
import com.ctrip.xpipe.redis.console.controller.api.data.MetaUpdateTest;
import com.ctrip.xpipe.redis.console.controller.api.data.MetaUpdateTest2;
import com.ctrip.xpipe.redis.console.controller.api.data.MetaUpdateTest3;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckPrepareRequestTest;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfoTest;
import com.ctrip.xpipe.redis.console.dal.ConcurrentDalTransactionTest;
import com.ctrip.xpipe.redis.console.dal.DalTransactionManagerTest;
import com.ctrip.xpipe.redis.console.dao.*;
import com.ctrip.xpipe.redis.console.health.action.HealthStatusTest;
import com.ctrip.xpipe.redis.console.health.clientconfig.CheckClusterTest;
import com.ctrip.xpipe.redis.console.health.sentinel.DefaultSentinelCollectorTest;
import com.ctrip.xpipe.redis.console.health.sentinel.SentinelHelloTest;
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
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifierTest;
import com.ctrip.xpipe.redis.console.notifier.MetaNotifyTaskTest;
import com.ctrip.xpipe.redis.console.service.MetaServiceTest;
import com.ctrip.xpipe.redis.console.service.impl.*;
import com.ctrip.xpipe.redis.console.service.meta.impl.ClusterMetaServiceImplTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 23, 2016
 */
@RunWith(Suite.class)
@SuiteClasses({
        SentinelHelloTest.class,
        DefaultSentinelCollectorTest.class,
        ConsoleCrossDcServerTest.class,
        HealthStatusTest.class,

        ClusterServiceImplTest.class,
        RedisServiceImplTest.class,
        KeepercontainerServiceImplTest.class,
        ShardServiceImplTest.class,
        SentinelServiceImplTest.class,
        ClusterMetaServiceImplTest.class,

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
        MultiShardMigrationTest.class,
        DefaultShardMigrationResultTest.class,
        ClusterCreateInfoTest.class,

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
        AlertPolicyManagerTest.class,

        ProxyDaoTest.class,
        RouteDaoTest.class,
        RouteServiceImplTest.class,
        RouteServiceImplTest.class,
        ProxyServiceImplTest.class
})
public class AllTests {

}
