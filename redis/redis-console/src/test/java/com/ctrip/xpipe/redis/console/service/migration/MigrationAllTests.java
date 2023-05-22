package com.ctrip.xpipe.redis.console.service.migration;

/**
 * @author lishanglin
 * date 2021/6/1
 */

import com.ctrip.xpipe.redis.console.beacon.DefaultMonitorServiceManagerTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.MigrationApiTest;
import com.ctrip.xpipe.redis.console.dao.MigrationEventConcurrentCreateTest;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon.BeaconClusterMonitorCheckTest;
import com.ctrip.xpipe.redis.console.notifier.DefaultClusterMonitorModifiedNotifierTest;
import com.ctrip.xpipe.redis.console.service.meta.impl.BeaconMetaServiceImplTest;
import com.ctrip.xpipe.redis.console.service.migration.cmd.beacon.*;
import com.ctrip.xpipe.redis.console.service.migration.impl.BeaconMigrationServiceImplTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
        MigrationEventConcurrentCreateTest.class,
        BeaconMetaServiceImplTest.class,
        BeaconMigrationServiceImplTest.class,
        MigrationApiTest.class,

        MigrationPreCheckCmdTest.class,
        MigrationFetchProcessingEventCmdTest.class,
        MigrationChooseTargetDcCmdTest.class,
        MigrationBuildEventCmdTest.class,
        MigrationDoExecuteCmdTest.class,

        DefaultMonitorServiceManagerTest.class,
        BeaconClusterMonitorCheckTest.class,
        DefaultClusterMonitorModifiedNotifierTest.class,
})
public class MigrationAllTests {
}
