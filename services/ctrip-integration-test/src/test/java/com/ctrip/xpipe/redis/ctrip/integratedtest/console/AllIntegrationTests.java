package com.ctrip.xpipe.redis.ctrip.integratedtest.console;

/**
 * @author lishanglin
 * date 2021/4/23
 */

import com.ctrip.xpipe.redis.ctrip.integratedtest.console.db.TransactionManagerTest;
import com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.BeaconSyncMigrationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        TransactionManagerTest.class,
        BeaconSyncMigrationTest.class
})
public class AllIntegrationTests {
}
