package com.ctrip.xpipe.redis.keeper;



import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateActiveTest;
import com.ctrip.xpipe.redis.keeper.impl.RedisKeeperServerStateBackupTest;
import com.ctrip.xpipe.redis.keeper.protocal.cmd.PsyncTest;
import com.ctrip.xpipe.redis.keeper.protocal.protocal.ArrayParserTest;
import com.ctrip.xpipe.redis.keeper.protocal.protocal.BulkStringParserTest;
import com.ctrip.xpipe.redis.keeper.protocal.protocal.SimpleStringParserTest;

/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:05:50 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	PsyncTest.class,
	SimpleStringParserTest.class,
	BulkStringParserTest.class,
	ArrayParserTest.class,
	RedisKeeperServerStateBackupTest.class,
	RedisKeeperServerStateActiveTest.class
})
public class AllTests {

}
