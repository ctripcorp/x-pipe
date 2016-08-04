package com.ctrip.xpipe.redis.keeper;




import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActiveTest;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateBackupTest;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateUnknownTest;
import com.ctrip.xpipe.redis.keeper.protocal.cmd.PsyncTest;
import com.ctrip.xpipe.redis.keeper.container.KeeperContainerServiceTest;

/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:05:50 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	PsyncTest.class,
	RedisKeeperServerStateBackupTest.class,
	RedisKeeperServerStateActiveTest.class,
	RedisKeeperServerStateUnknownTest.class,
	KeeperContainerServiceTest.class
})
public class AllTests {

}
