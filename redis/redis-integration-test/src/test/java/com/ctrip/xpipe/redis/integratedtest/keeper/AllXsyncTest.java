package com.ctrip.xpipe.redis.integratedtest.keeper;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        KeeperGapAllowedSync.class,
        KeeperSwitchXsyncTest.class,
        KeeperSwitchMultDcTest.class,
        KeeperSwitchProto.class,
        KeeperSwitchXsyncTest.class,
        KeeperXsyncGapTest.class,
        KeeperXsyncTest.class,
        MasterSwitchMultDcTest.class,
        KeeperXSyncCrossRegionTest.class,
        GtidCmdSearcherKeeperTest.class,
})
public class AllXsyncTest {
}
