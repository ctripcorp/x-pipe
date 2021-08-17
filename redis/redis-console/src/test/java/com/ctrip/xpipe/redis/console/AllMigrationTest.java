package com.ctrip.xpipe.redis.console;

import com.ctrip.xpipe.redis.console.dao.MigrationClusterDaoTest;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDaoTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableCheckTest;
import com.ctrip.xpipe.redis.console.migration.MultiClusterMigrationTest;
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
        MultiClusterMigrationTest.class,

        AdvancedDcMetaServiceTestForRoute.class,
        AdvancedDcMetaServiceTest.class,
        ClusterMetaServiceImplTest.class,

        MigrationSystemAvailableCheckTest.class,
        DefaultCheckMigrationCommandBuilderTest.class
})
public class AllMigrationTest {
}
